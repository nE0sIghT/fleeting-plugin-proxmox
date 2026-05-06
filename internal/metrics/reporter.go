package metrics

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"
)

const (
	DefaultReporterInterval = 15 * time.Second
	defaultReporterRetry    = time.Second
	defaultReporterTimeout  = 5 * time.Second
)

type ReporterConfig struct {
	SocketPath string
	Interval   time.Duration
	Identity   Identity
	Collect    func(context.Context) (Snapshot, error)
	Info       func(string, ...any)
	Warn       func(string, ...any)
}

type Reporter struct {
	cfg ReporterConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu          sync.Mutex
	lastSendSet bool
	lastSendOK  bool
}

func NewReporter(cfg ReporterConfig) *Reporter {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultReporterInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Reporter{cfg: cfg, ctx: ctx, cancel: cancel}
}

func (r *Reporter) Start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.run()
	}()
}

func (r *Reporter) Shutdown(ctx context.Context) {
	r.cancel()
	r.wg.Wait()

	if r.cfg.SocketPath == "" {
		return
	}
	snapshot := Snapshot{
		Version:           1,
		Identity:          r.cfg.Identity,
		TimestampUnix:     time.Now().Unix(),
		Up:                false,
		LastScrapeSuccess: false,
	}
	_ = r.send(ctx, snapshot)
}

func (r *Reporter) run() {
	for {
		err := r.publish(r.ctx)
		r.recordSendResult(err)

		wait := r.cfg.Interval
		if err != nil && wait > defaultReporterRetry {
			wait = defaultReporterRetry
		}

		timer := time.NewTimer(wait)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Reporter) publish(ctx context.Context) error {
	if r.cfg.SocketPath == "" {
		return nil
	}

	collectCtx, cancel := context.WithTimeout(ctx, r.cfg.Interval)
	defer cancel()

	conn, err := r.dial(collectCtx)
	if err != nil {
		return err
	}
	defer conn.Close()

	snapshot, err := r.cfg.Collect(collectCtx)
	if err != nil {
		snapshot = Snapshot{
			Version:           1,
			Identity:          r.cfg.Identity,
			TimestampUnix:     time.Now().Unix(),
			Up:                true,
			LastScrapeSuccess: false,
			LastError:         err.Error(),
		}
	} else {
		snapshot.Version = 1
		snapshot.Identity = r.cfg.Identity
		snapshot.TimestampUnix = time.Now().Unix()
		snapshot.Up = true
		snapshot.LastScrapeSuccess = true
	}

	encoder := json.NewEncoder(conn)
	return encoder.Encode(snapshot)
}

func (r *Reporter) send(ctx context.Context, snapshot Snapshot) error {
	sendCtx, cancel := context.WithTimeout(ctx, defaultReporterTimeout)
	defer cancel()

	conn, err := r.dial(sendCtx)
	if err != nil {
		return err
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	return encoder.Encode(snapshot)
}

func (r *Reporter) dial(ctx context.Context) (net.Conn, error) {
	dialer := net.Dialer{Timeout: defaultReporterTimeout}
	return dialer.DialContext(ctx, "unix", r.cfg.SocketPath)
}

func (r *Reporter) recordSendResult(err error) {
	ok := err == nil

	r.mu.Lock()
	wasSet := r.lastSendSet
	changed := wasSet && r.lastSendOK != ok
	firstFailure := !wasSet && !ok
	r.lastSendSet = true
	r.lastSendOK = ok
	r.mu.Unlock()

	if !changed && !firstFailure {
		return
	}
	if ok {
		if r.cfg.Info != nil {
			r.cfg.Info("metrics exporter connection restored", "socket", r.cfg.SocketPath)
		}
		return
	}
	if r.cfg.Warn != nil {
		r.cfg.Warn("metrics exporter unavailable", "socket", r.cfg.SocketPath, "error", err)
	}
}
