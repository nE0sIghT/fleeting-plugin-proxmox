package metrics

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

type controlSession struct {
	identity Identity
	nodes    []string
	conn     net.Conn
	updates  chan struct{}
	epoch    string
	revision uint64
	snapshot Snapshot
	received time.Time
}

type persistedControl struct {
	Version  int           `json:"version"`
	Revision uint64        `json:"revision"`
	Nodes    []NodeRef     `json:"nodes"`
	Fleets   []FleetStatus `json:"fleets,omitempty"`
}

func (e *Exporter) loadControl() error {
	data, err := os.ReadFile(e.cfg.ControlStateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read control state: %w", err)
	}
	var saved persistedControl
	if err := json.Unmarshal(data, &saved); err != nil {
		return fmt.Errorf("decode control state: %w", err)
	}
	if saved.Version != 1 {
		return fmt.Errorf("unsupported or missing control state version")
	}
	for _, node := range saved.Nodes {
		if node.Cluster == "" || node.Node == "" {
			return fmt.Errorf("invalid node in control state")
		}
	}
	e.controlState.Revision = saved.Revision
	e.controlState.Nodes = saved.Nodes
	for i, fleet := range saved.Fleets {
		e.sessions[fmt.Sprintf("restored-%d", i)] = &controlSession{identity: fleet.Identity, nodes: fleet.ConfiguredNodes}
	}
	return nil
}

// controlMu protects both persisted desired state and session acknowledgements.
func (e *Exporter) changeDrain(req ControlRequest) error {
	e.controlMu.Lock()
	defer e.controlMu.Unlock()
	if len(req.Nodes) == 0 {
		return fmt.Errorf("at least one cluster/node is required")
	}
	nodes := map[string]NodeRef{}
	for _, n := range e.controlState.Nodes {
		nodes[n.Key()] = n
	}
	for _, n := range req.Nodes {
		if n.Cluster == "" || n.Node == "" || strings.ContainsAny(n.Cluster+n.Node, "/\x00\r\n") {
			return fmt.Errorf("invalid cluster/node")
		}
		known := false
		for _, s := range e.sessions {
			if s.identity.Cluster == n.Cluster {
				for _, node := range s.nodes {
					if node == n.Node {
						known = true
					}
				}
			}
		}
		if _, ok := nodes[n.Key()]; ok {
			known = true
		}
		if !known {
			return fmt.Errorf("unknown node %s; wait for its fleet to register", n.Key())
		}
		if req.Action == "drain" {
			nodes[n.Key()] = n
		} else {
			delete(nodes, n.Key())
		}
	}
	next := DrainState{Epoch: e.controlState.Epoch, Revision: e.controlState.Revision + 1, Nodes: []NodeRef{}}
	for _, n := range nodes {
		next.Nodes = append(next.Nodes, n)
	}
	sort.Slice(next.Nodes, func(i, j int) bool { return next.Nodes[i].Key() < next.Nodes[j].Key() })
	if err := e.saveControlLocked(next); err != nil {
		return err
	}
	e.controlState = next
	for _, s := range e.sessions {
		if s.conn != nil {
			select {
			case s.updates <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

func (e *Exporter) saveControlLocked(state DrainState) error {
	saved := persistedControl{Version: 1, Revision: state.Revision, Nodes: state.Nodes}
	known := map[string]bool{}
	for _, s := range e.sessions {
		if !known[s.identity.Key()] {
			saved.Fleets = append(saved.Fleets, FleetStatus{Identity: s.identity, ConfiguredNodes: s.nodes})
			known[s.identity.Key()] = true
		}
	}
	data, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return err
	}
	if err := writeAtomicFile(e.cfg.ControlStateFile, data, 0o640); err != nil {
		return err
	}
	return nil
}

func (e *Exporter) serveControl(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		e.startConnHandler(conn, func(conn net.Conn) {
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			var req ControlRequest
			var response ControlResponse
			if err := decodeLine(newIPCScanner(conn), &req); err != nil {
				return
			}
			if req.Version != 1 {
				response.Error = "unsupported control protocol version"
			} else {
				switch req.Action {
				case "status":
				case "drain", "undrain":
					if err := e.changeDrain(req); err != nil {
						response.Error = err.Error()
					}
				default:
					response.Error = "unknown control action"
				}
			}
			response.Status = e.controlStatus(time.Now())
			_ = json.NewEncoder(conn).Encode(response)
		})
	}
}

func newIPCScanner(conn net.Conn) *bufio.Scanner {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	return scanner
}

func decodeLine(scanner *bufio.Scanner, target any) error {
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return err
		}
		return fmt.Errorf("IPC connection closed")
	}
	return json.Unmarshal(scanner.Bytes(), target)
}

func (e *Exporter) handleSession(conn net.Conn, scanner *bufio.Scanner, hello Snapshot) {
	if hello.Session == "" || len(hello.ConfiguredNodes) == 0 {
		return
	}
	s := &controlSession{identity: hello.Identity, nodes: hello.ConfiguredNodes, conn: conn, updates: make(chan struct{}, 1)}
	e.controlMu.Lock()
	if e.stopping {
		e.controlMu.Unlock()
		return
	}
	// A new process supersedes disconnected processes of the same fleet. Active
	// duplicates stay visible and must each acknowledge the desired state.
	for id, old := range e.sessions {
		if old.identity == s.identity && old.conn == nil {
			delete(e.sessions, id)
		}
	}
	if old := e.sessions[hello.Session]; old != nil && old.conn != nil {
		_ = old.conn.Close()
	}
	e.sessions[hello.Session] = s
	if err := e.saveControlLocked(e.controlState); err != nil {
		s.conn = nil
		e.controlMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_ = json.NewEncoder(conn).Encode(controlMessage{Type: "error", Error: err.Error()})
		return
	}
	e.controlMu.Unlock()
	defer func() {
		e.controlMu.Lock()
		s.conn = nil
		e.controlMu.Unlock()
	}()
	done := make(chan struct{})
	writerDone := make(chan struct{})
	defer func() {
		close(done)
		_ = conn.Close()
		<-writerDone
	}()
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			e.controlMu.Lock()
			state := e.controlState
			e.controlMu.Unlock()
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := json.NewEncoder(conn).Encode(controlMessage{Type: "state", State: state}); err != nil {
				_ = conn.Close()
				return
			}
			select {
			case <-done:
				return
			case <-s.updates:
			case <-ticker.C:
			}
		}
	}()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		var snapshot Snapshot
		if err := decodeLine(scanner, &snapshot); err != nil {
			return
		}
		if snapshot.Version != 2 || snapshot.Session != hello.Session || snapshot.Identity != hello.Identity {
			return
		}
		e.controlMu.Lock()
		if snapshot.Type == "ack" {
			s.epoch = snapshot.ControlEpoch
			s.revision = snapshot.ControlRevision
		} else if snapshot.Type == "snapshot" {
			s.snapshot = snapshot
			s.received = time.Now()
		}
		e.controlMu.Unlock()
		if snapshot.Type == "snapshot" {
			e.storeSnapshot(snapshot)
		}
	}
}

func (e *Exporter) controlStatus(now time.Time) ControlStatus {
	result := ControlStatus{GeneratedUnix: now.Unix(), Problems: e.problemStatus(now)}
	e.controlMu.Lock()
	result.State = e.controlState
	refs := map[string]NodeRef{}
	for _, n := range result.State.Nodes {
		refs[n.Key()] = n
	}
	for session, s := range e.sessions {
		fleet := FleetStatus{Identity: s.identity, Session: session, Connected: s.conn != nil, Applied: s.conn != nil && s.epoch == result.State.Epoch && s.revision == result.State.Revision,
			Fresh:           !s.received.IsZero() && now.Sub(s.received) <= e.cfg.StaleAfter && s.snapshot.Up && s.snapshot.LastScrapeSuccess,
			ConfiguredNodes: append([]string{}, s.nodes...), Snapshot: s.snapshot}
		if !s.received.IsZero() {
			fleet.ReceivedUnix = s.received.Unix()
		}
		result.Fleets = append(result.Fleets, fleet)
		for _, node := range s.nodes {
			n := NodeRef{s.identity.Cluster, node}
			refs[n.Key()] = n
		}
	}
	e.controlMu.Unlock()
	// Older plugins can still report metrics, but cannot confirm drain.
	e.mu.Lock()
	for _, stored := range e.snapshots {
		if stored.Snapshot.Version != 1 {
			continue
		}
		found := false
		for _, f := range result.Fleets {
			if f.Identity == stored.Snapshot.Identity {
				found = true
			}
		}
		if found {
			continue
		}
		f := FleetStatus{Identity: stored.Snapshot.Identity, Snapshot: stored.Snapshot, ReceivedUnix: stored.ReceivedUnix}
		for _, n := range stored.Snapshot.Nodes {
			f.ConfiguredNodes = append(f.ConfiguredNodes, n.Node)
			ref := NodeRef{f.Identity.Cluster, n.Node}
			refs[ref.Key()] = ref
		}
		result.Fleets = append(result.Fleets, f)
	}
	e.mu.Unlock()
	sort.Slice(result.Fleets, func(i, j int) bool {
		a, b := result.Fleets[i], result.Fleets[j]
		if a.Identity.Key() != b.Identity.Key() {
			return a.Identity.Key() < b.Identity.Key()
		}
		return a.Session < b.Session
	})
	for _, ref := range refs {
		n := DrainStatus{NodeRef: ref, Applied: true, Empty: true}
		for _, drained := range result.State.Nodes {
			if drained == ref {
				n.Drained = true
			}
		}
		count := 0
		for _, f := range result.Fleets {
			if f.Identity.Cluster != ref.Cluster {
				continue
			}
			includes := false
			for _, node := range f.ConfiguredNodes {
				if node == ref.Node {
					includes = true
				}
			}
			if !includes {
				continue
			}
			count++
			if !f.Applied {
				n.Applied = false
			}
			current := f.Fresh && f.Snapshot.ControlEpoch == result.State.Epoch && f.Snapshot.ControlRevision == result.State.Revision && f.Snapshot.NodeErrors[ref.Node] == ""
			if !f.Applied || !current {
				n.Empty = false
				n.Awaiting = append(n.Awaiting, f.Identity.Key()+" ["+f.Session+"]")
			}
			for _, instance := range f.Snapshot.InstanceDetails {
				if instance.Node == ref.Node {
					n.Instances++
				}
			}
			n.Pending += f.Snapshot.NodePending[ref.Node]
			n.Templates += f.Snapshot.NodeTemplates[ref.Node]
		}
		if count == 0 {
			n.Applied = false
			n.Empty = false
			n.Awaiting = append(n.Awaiting, "no registered fleets")
		}
		n.Empty = n.Empty && n.Instances == 0 && n.Pending == 0
		result.Nodes = append(result.Nodes, n)
	}
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].Key() < result.Nodes[j].Key() })
	return result
}

func RequestControl(ctx context.Context, socket string, request ControlRequest) (ControlStatus, error) {
	request.Version = 1
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return ControlStatus{}, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return ControlStatus{}, err
	}
	var response ControlResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return ControlStatus{}, err
	}
	if response.Error != "" {
		return response.Status, errors.New(response.Error)
	}
	return response.Status, nil
}
