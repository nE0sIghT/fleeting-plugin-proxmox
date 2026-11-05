package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

func (r *Reporter) stream(ctx context.Context) error {
	conn, err := r.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var writeMu sync.Mutex
	send := func(value any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(defaultReporterTimeout))
		return json.NewEncoder(conn).Encode(value)
	}
	base := Snapshot{Version: 2, Session: r.problemSource, Identity: r.cfg.Identity, ConfiguredNodes: r.cfg.ControlNodes}
	hello := base
	hello.Type = "hello"
	if err := send(hello); err != nil {
		return err
	}
	var stateMu sync.Mutex
	var applied DrainState
	wake := make(chan struct{}, 1)
	errs := make(chan error, 1)
	readerDone := make(chan struct{})
	defer func() { _ = conn.Close(); <-readerDone }()
	go func() {
		defer close(readerDone)
		scanner := newIPCScanner(conn)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
			var message controlMessage
			if err := decodeLine(scanner, &message); err != nil {
				errs <- err
				return
			}
			if message.Type != "state" || message.State.Epoch == "" {
				if message.Error != "" {
					errs <- fmt.Errorf("control service: %s", message.Error)
					return
				}
				errs <- fmt.Errorf("invalid control state")
				return
			}
			stateMu.Lock()
			changed := applied.Epoch != message.State.Epoch || applied.Revision != message.State.Revision
			stateMu.Unlock()
			if changed {
				r.cfg.ApplyControl(message.State)
			}
			stateMu.Lock()
			applied = message.State
			stateMu.Unlock()
			ack := base
			ack.Type = "ack"
			ack.ControlEpoch = message.State.Epoch
			ack.ControlRevision = message.State.Revision
			if err := send(ack); err != nil {
				errs <- err
				return
			}
			if changed {
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}()
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return err
		case <-wake:
		case <-ticker.C:
		}
		stateMu.Lock()
		state := applied
		stateMu.Unlock()
		if state.Epoch == "" {
			continue
		}
		collectCtx, cancel := context.WithTimeout(ctx, r.cfg.Interval)
		snapshot, err := r.cfg.Collect(collectCtx)
		cancel()
		if err != nil {
			r.setCollectionProblem(err)
			snapshot = Snapshot{LastError: err.Error()}
		} else {
			r.clearCollectionProblem()
		}
		snapshot.Version = 2
		snapshot.Type = "snapshot"
		snapshot.Session = base.Session
		snapshot.Identity = base.Identity
		snapshot.ConfiguredNodes = base.ConfiguredNodes
		snapshot.ControlEpoch = state.Epoch
		snapshot.ControlRevision = state.Revision
		snapshot.TimestampUnix = time.Now().Unix()
		snapshot.Up = true
		snapshot.LastScrapeSuccess = err == nil
		writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(defaultReporterTimeout))
		err = r.encodeSnapshot(conn, snapshot)
		writeMu.Unlock()
		if err != nil {
			return err
		}
		r.recordSendResult(nil)
	}
}
