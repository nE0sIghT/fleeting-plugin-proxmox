package metrics

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testControlConfig(t *testing.T) ExporterConfig {
	t.Helper()
	dir := t.TempDir()
	return ExporterConfig{ListenAddress: "127.0.0.1:0", SocketPath: filepath.Join(dir, "metrics.sock"), ControlSocket: filepath.Join(dir, "control.sock"), ControlStateFile: filepath.Join(dir, "state.json"), ExceptionsFile: filepath.Join(dir, "exceptions"), StaleAfter: time.Second}
}

func startControlExporter(t *testing.T, cfg ExporterConfig) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunExporter(ctx, cfg) }()
	require.Eventually(t, func() bool {
		_, err := RequestControl(context.Background(), cfg.ControlSocket, ControlRequest{Action: "status"})
		return err == nil
	}, time.Second, 10*time.Millisecond)
	var stopped atomic.Bool
	stop := func() {
		if stopped.Swap(true) {
			return
		}
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Error("exporter did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func getControlStatus(t *testing.T, cfg ExporterConfig) ControlStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := RequestControl(ctx, cfg.ControlSocket, ControlRequest{Action: "status"})
	require.NoError(t, err)
	return status
}

func TestExporterShutdownWaitsForSnapshotWrite(t *testing.T) {
	t.Parallel()
	cfg := testControlConfig(t)
	exporter := NewExporter(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- exporter.Run(ctx) }()
	locked := false
	stopped := false
	t.Cleanup(func() {
		if locked {
			exporter.fileMu.Unlock()
		}
		cancel()
		if !stopped {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("exporter did not stop")
			}
		}
	})
	require.Eventually(t, func() bool {
		_, err := RequestControl(context.Background(), cfg.ControlSocket, ControlRequest{Action: "status"})
		return err == nil
	}, time.Second, 10*time.Millisecond)
	// Shutdown must also close accepted connections that have not sent a request yet.
	pending, err := net.Dial("unix", cfg.ControlSocket)
	require.NoError(t, err)
	defer pending.Close()
	exporter.fileMu.Lock()
	locked = true
	conn, err := net.Dial("unix", cfg.SocketPath)
	require.NoError(t, err)
	defer conn.Close()
	identity := Identity{Cluster: "lab", Pool: "ci", Group: "runner"}
	require.NoError(t, json.NewEncoder(conn).Encode(Snapshot{Version: 1, Identity: identity}))
	require.Eventually(t, func() bool {
		exporter.mu.Lock()
		defer exporter.mu.Unlock()
		_, ok := exporter.snapshots[identity.Key()]
		return ok
	}, time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		stopped = true
		t.Fatalf("Run returned before the pending snapshot write finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	exporter.fileMu.Unlock()
	locked = false
	select {
	case err := <-done:
		stopped = true
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("exporter did not finish shutdown after the write was released")
	}
}

func TestControlDrainReconnectAndRestart(t *testing.T) {
	t.Parallel()
	cfg := testControlConfig(t)
	var gates [2]PlacementControl
	var vmCount atomic.Int32
	vmCount.Store(1)
	for i := range gates {
		gates[i].Enable()
	}
	// Reporters may start before the exporter creates its socket.
	for i := range gates {
		i := i
		reporter := NewReporter(ReporterConfig{SocketPath: cfg.SocketPath, Interval: 20 * time.Millisecond, Identity: Identity{Cluster: "lab", Pool: "ci", Group: []string{"one", "two"}[i]}, ControlNodes: []string{"node1", "node2"}, ApplyControl: func(s DrainState) { gates[i].Apply(s, "lab") }, Collect: func(context.Context) (Snapshot, error) {
			s := Snapshot{NodePending: map[string]int{}, NodeTemplates: map[string]int{"node1": 1}}
			if vmCount.Load() > 0 {
				s.InstanceDetails = []InstanceSnapshot{{ID: "node1/5000", Node: "node1", State: "running"}}
			}
			return s, nil
		}})
		reporter.Start()
		t.Cleanup(func() { reporter.Shutdown(context.Background()) })
	}
	require.ErrorIs(t, gates[0].Check("node1"), ErrPlacementPaused)
	stop := startControlExporter(t, cfg)
	require.Eventually(t, func() bool {
		s := getControlStatus(t, cfg)
		return len(s.Fleets) == 2 && s.Fleets[0].Applied && s.Fleets[1].Applied && s.Fleets[0].Fresh && s.Fleets[1].Fresh
	}, 2*time.Second, 10*time.Millisecond)
	_, err := RequestControl(context.Background(), cfg.ControlSocket, ControlRequest{Action: "drain", Nodes: []NodeRef{{"lab", "node1"}}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return gates[0].Check("node1") != nil && gates[1].Check("node1") != nil }, time.Second, 10*time.Millisecond)
	require.NoError(t, gates[0].Check("node2"))
	require.Eventually(t, func() bool {
		s := getControlStatus(t, cfg)
		return s.Nodes[0].Applied && s.Nodes[0].Instances == 2 && !s.Nodes[0].Empty
	}, time.Second, 10*time.Millisecond)
	vmCount.Store(0)
	require.Eventually(t, func() bool { s := getControlStatus(t, cfg); return s.Nodes[0].Empty && s.Nodes[0].Templates == 2 }, time.Second, 10*time.Millisecond)
	before := getControlStatus(t, cfg).State
	stop()
	// A lost socket must not undo an already applied drain.
	require.ErrorIs(t, gates[0].Check("node1"), ErrPlacementPaused)
	require.NoError(t, gates[0].Check("node2"))
	startControlExporter(t, cfg)
	require.Eventually(t, func() bool {
		s := getControlStatus(t, cfg)
		return s.State.Epoch != before.Epoch && s.State.Revision == before.Revision && len(s.Fleets) == 2 && s.Nodes[0].Applied && s.Nodes[0].Empty
	}, 2*time.Second, 10*time.Millisecond)
	_, err = RequestControl(context.Background(), cfg.ControlSocket, ControlRequest{Action: "undrain", Nodes: []NodeRef{{"lab", "node1"}}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return gates[0].Check("node1") == nil && gates[1].Check("node1") == nil }, time.Second, 10*time.Millisecond)
}

func TestControlMissingAckStaleSnapshotAndOfflineFleet(t *testing.T) {
	t.Parallel()
	e := NewExporter(testControlConfig(t))
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	e.controlState.Nodes = []NodeRef{{"lab", "node1"}}
	s := &controlSession{identity: Identity{"lab", "ci", "one"}, nodes: []string{"node1"}, conn: conn, received: time.Now(), snapshot: Snapshot{Up: true, LastScrapeSuccess: true}}
	e.sessions["one"] = s
	status := e.controlStatus(time.Now())
	require.False(t, status.Nodes[0].Applied)
	require.False(t, status.Nodes[0].Empty)
	s.epoch = e.controlState.Epoch
	s.revision = e.controlState.Revision
	status = e.controlStatus(time.Now())
	require.True(t, status.Nodes[0].Applied)
	require.False(t, status.Nodes[0].Empty, "old snapshot must not confirm emptiness")
	s.snapshot.ControlEpoch = s.epoch
	s.snapshot.ControlRevision = s.revision
	status = e.controlStatus(time.Now())
	require.True(t, status.Nodes[0].Empty)
	s.snapshot.NodeErrors = map[string]string{"node1": "offline"}
	status = e.controlStatus(time.Now())
	require.False(t, status.Nodes[0].Empty)
	s.snapshot.NodeErrors = nil
	status = e.controlStatus(time.Now().Add(2 * time.Second))
	require.False(t, status.Nodes[0].Empty)
	s.conn = nil
	status = e.controlStatus(time.Now())
	require.False(t, status.Nodes[0].Applied)
	require.False(t, status.Nodes[0].Empty)
}

func TestControlPersistenceFailureDoesNotApplyDrain(t *testing.T) {
	t.Parallel()
	cfg := testControlConfig(t)
	require.NoError(t, os.WriteFile(cfg.ControlStateFile, []byte("blocked"), 0o600))
	cfg.ControlStateFile = filepath.Join(cfg.ControlStateFile, "state.json")
	e := NewExporter(cfg)
	e.sessions["one"] = &controlSession{identity: Identity{"lab", "ci", "one"}, nodes: []string{"node1"}}
	require.Error(t, e.changeDrain(ControlRequest{Action: "drain", Nodes: []NodeRef{{"lab", "node1"}}}))
	require.Empty(t, e.controlState.Nodes)
	require.Zero(t, e.controlState.Revision)
}

func TestDrainAppliesAfterCloneSubmissionAndBlocksNextClone(t *testing.T) {
	t.Parallel()
	var gate PlacementControl
	gate.Enable()
	gate.Apply(DrainState{}, "lab")
	release, err := gate.BeginClone("node1")
	require.NoError(t, err)
	done := make(chan struct{})
	started := make(chan struct{})
	go func() { close(started); gate.Apply(DrainState{Nodes: []NodeRef{{"lab", "node1"}}}, "lab"); close(done) }()
	<-started
	select {
	case <-done:
		t.Fatal("drain acknowledged during clone submission")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain did not apply")
	}
	_, err = gate.BeginClone("node1")
	require.ErrorIs(t, err, ErrPlacementPaused)
	require.NoError(t, gate.Check("node2"))
}
