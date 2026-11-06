package operator

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"
)

type capturedScreen struct {
	tcell.SimulationScreen
	mu   sync.Mutex
	text string
}

func (s *capturedScreen) capture() {
	cells, width, _ := s.GetContents()
	var out strings.Builder
	for i, cell := range cells {
		if width > 0 && i%width == 0 {
			out.WriteByte('\n')
		}
		if len(cell.Runes) > 0 {
			out.WriteRune(cell.Runes[0])
		} else {
			out.WriteByte(' ')
		}
	}
	s.text = out.String()
}

func (s *capturedScreen) Show() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SimulationScreen.Show()
	s.capture()
}
func (s *capturedScreen) Sync() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SimulationScreen.Sync()
	s.capture()
}
func (s *capturedScreen) Text() string { s.mu.Lock(); defer s.mu.Unlock(); return s.text }

func TestParseDrainAndFlags(t *testing.T) {
	action, opts, nodes, err := Parse([]string{"drain", "lab/node1", "lab/node2", "--wait", "--timeout", "30m", "--socket", "/tmp/control.sock", "--json"})
	require.NoError(t, err)
	require.Equal(t, "drain", action)
	require.True(t, opts.Wait)
	require.True(t, opts.JSON)
	require.Equal(t, 30*time.Minute, opts.Timeout)
	require.Len(t, nodes, 2)
	for _, args := range [][]string{{"drain"}, {"drain", "lab/node/extra"}, {"undrain", "lab/node", "--wait"}, {"status", "--timeout", "0"}, {"status", "--unknown"}} {
		_, _, _, err := Parse(args)
		require.Error(t, err)
	}
}

func TestWaitDoesNotConfirmMissingFleetAndDetectsSupersession(t *testing.T) {
	ref := metrics.NodeRef{Cluster: "lab", Node: "one"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	status := metrics.ControlStatus{Nodes: []metrics.DrainStatus{{NodeRef: ref, Drained: true, Applied: false}}}
	_, err := WaitNodes(ctx, "", status, []metrics.NodeRef{ref}, true, false)
	require.ErrorIs(t, err, context.Canceled)
	status.Nodes[0].Drained = false
	_, err = WaitNodes(context.Background(), "", status, []metrics.NodeRef{ref}, true, false)
	require.ErrorContains(t, err, "superseded")
}

func TestRenderDoesNotSumPhysicalResourcesAndEscapesControlCharacters(t *testing.T) {
	status := metrics.ControlStatus{Fleets: []metrics.FleetStatus{
		{Identity: metrics.Identity{Cluster: "lab", Group: "one"}, Snapshot: metrics.Snapshot{Nodes: []metrics.NodeSnapshot{{Node: "node1", TotalCPUCores: 16}}}},
		{Identity: metrics.Identity{Cluster: "lab", Group: "two"}, Snapshot: metrics.Snapshot{Nodes: []metrics.NodeSnapshot{{Node: "node1", TotalCPUCores: 16}}}},
	}}
	var out strings.Builder
	Render(&out, "nodes", status, Options{})
	require.Equal(t, 2, strings.Count(out.String(), "16.0"))
	require.Equal(t, "hello [2J world", safe("hello\x1b[2J\nworld"))
}

func TestTUIStartsResizesAndExitsWithoutExporter(t *testing.T) {
	screen := tcell.NewSimulationScreen("")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runTUI(ctx, Options{Socket: "/nonexistent/control.sock", Timeout: time.Second}, screen)
	}()
	// Cancellation also exercises a wake-up while the status worker reconnects.
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("TUI failed to stop")
	}
}

func TestTUIRendersNodesAndSendsDrain(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "control.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	serverDone := make(chan struct{})
	var drained atomic.Bool
	go func() {
		defer close(serverDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			var req metrics.ControlRequest
			if json.NewDecoder(conn).Decode(&req) == nil {
				if req.Action == "drain" && len(req.Nodes) == 1 && req.Nodes[0].Node == "node05" {
					drained.Store(true)
				}
				_ = json.NewEncoder(conn).Encode(metrics.ControlResponse{Status: metrics.ControlStatus{
					GeneratedUnix: time.Now().Unix(), Problems: metrics.ProblemStatus{Status: "DEGRADED", Recent: 2, Problems: []metrics.ProblemView{
						{State: metrics.ProblemRecent, Cluster: "cluster02", Pool: "ci", Group: "other", Node: "node06", Code: "clone_failed", Message: "storage lock timeout"},
						{State: metrics.ProblemRecent, Cluster: "cluster01", Pool: "ci", Group: "one", Code: "reconcile_failed", Message: "API unavailable"},
					}},
					Nodes: []metrics.DrainStatus{{NodeRef: metrics.NodeRef{Cluster: "cluster01", Node: "node05"}, Drained: drained.Load(), Applied: true}},
				}})
			}
			_ = conn.Close()
		}
	}()
	defer func() { _ = listener.Close(); <-serverDone }()
	screen := &capturedScreen{SimulationScreen: tcell.NewSimulationScreen("")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runTUI(ctx, Options{Socket: socket, Timeout: time.Second}, screen) }()
	content := screen.Text
	require.Eventually(t, func() bool { return strings.Contains(content(), "node05") }, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return strings.Contains(content(), "active 0 | recent 2") }, time.Second, 10*time.Millisecond)
	screen.SetSize(160, 40)
	_ = screen.PostEvent(tcell.NewEventResize(160, 40))
	_ = screen.PostEvent(tcell.NewEventKey(tcell.KeyRune, '4', tcell.ModNone))
	require.Eventually(t, func() bool {
		return strings.Contains(content(), "clone_failed") && strings.Contains(content(), "reconcile_failed")
	}, time.Second, 10*time.Millisecond)
	screen.SetSize(60, 20)
	_ = screen.PostEvent(tcell.NewEventResize(60, 20))
	_ = screen.PostEvent(tcell.NewEventKey(tcell.KeyRune, ' ', tcell.ModNone))
	_ = screen.PostEvent(tcell.NewEventKey(tcell.KeyRune, 'd', tcell.ModNone))
	require.Eventually(t, drained.Load, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return strings.Contains(content(), "drained") }, time.Second, 10*time.Millisecond)
	_ = screen.PostEvent(tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("TUI did not exit")
	}
}
