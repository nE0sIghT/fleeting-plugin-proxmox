package metrics

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReporterWaitsForSocketAndPublishes(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "metrics.sock")
	reporter := NewReporter(ReporterConfig{
		SocketPath: socketPath,
		Interval:   10 * time.Millisecond,
		Identity:   Identity{Cluster: "prod", Pool: "ci", Group: "arm64"},
		Collect: func(context.Context) (Snapshot, error) {
			return Snapshot{Instances: map[string]int{"running": 1}}, nil
		},
	})
	reporter.Start()
	defer reporter.Shutdown(context.Background())

	time.Sleep(30 * time.Millisecond)

	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer listener.Close()

	require.NoError(t, listener.(*net.UnixListener).SetDeadline(time.Now().Add(time.Second)))
	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()

	var snapshot Snapshot
	require.NoError(t, json.NewDecoder(conn).Decode(&snapshot))
	require.Equal(t, Identity{Cluster: "prod", Pool: "ci", Group: "arm64"}, snapshot.Identity)
	require.True(t, snapshot.Up)
	require.True(t, snapshot.LastScrapeSuccess)
	require.Equal(t, 1, snapshot.Instances["running"])
}

func TestReporterCoalescesProblemsWhileSocketIsUnavailable(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "metrics.sock")
	reporter := NewReporter(ReporterConfig{
		SocketPath: socketPath,
		Interval:   10 * time.Millisecond,
		Identity:   Identity{Cluster: "prod", Pool: "ci", Group: "arm64"},
		Collect: func(context.Context) (Snapshot, error) {
			return Snapshot{}, nil
		},
	})
	reporter.ReportProblem(ProblemEvent{
		Code:     "clone_failed",
		State:    ProblemRecent,
		Phase:    "clone",
		Node:     "pve01",
		Storage:  "nvme0",
		Instance: "pve01/5000",
		Message:  "first failure",
	})
	reporter.ReportProblem(ProblemEvent{
		Code:     "clone_failed",
		State:    ProblemRecent,
		Phase:    "clone",
		Node:     "pve01",
		Storage:  "nvme0",
		Instance: "pve01/5001",
		Message:  "second failure",
	})
	reporter.Start()
	defer reporter.Shutdown(context.Background())

	time.Sleep(30 * time.Millisecond)
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer listener.Close()

	require.NoError(t, listener.(*net.UnixListener).SetDeadline(time.Now().Add(time.Second)))
	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()

	var snapshot Snapshot
	require.NoError(t, json.NewDecoder(conn).Decode(&snapshot))
	require.Len(t, snapshot.ProblemEvents, 1)
	require.Equal(t, uint64(2), snapshot.ProblemEvents[0].Occurrences)
	require.Equal(t, "pve01/5001", snapshot.ProblemEvents[0].Instance)
	require.Equal(t, "second failure", snapshot.ProblemEvents[0].Message)
}

func TestReporterPreservesProblemOccurrenceResolvedBeforeDelivery(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "metrics.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer listener.Close()

	reporter := NewReporter(ReporterConfig{
		SocketPath: socketPath,
		Identity:   Identity{Cluster: "prod", Pool: "ci", Group: "arm64"},
	})
	reporter.ReportProblem(ProblemEvent{
		Code:    "init_failed",
		State:   ProblemActive,
		Phase:   "init",
		Message: "API unavailable",
	})
	reporter.ReportProblem(ProblemEvent{
		Code:  "init_failed",
		State: ProblemResolved,
		Phase: "init",
	})

	done := make(chan struct{})
	go func() {
		reporter.FlushProblems(context.Background(), true)
		close(done)
	}()

	require.NoError(t, listener.(*net.UnixListener).SetDeadline(time.Now().Add(time.Second)))
	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()

	var snapshot Snapshot
	require.NoError(t, json.NewDecoder(conn).Decode(&snapshot))
	require.Len(t, snapshot.ProblemEvents, 1)
	require.Equal(t, ProblemResolved, snapshot.ProblemEvents[0].State)
	require.Equal(t, uint64(1), snapshot.ProblemEvents[0].Occurrences)
	require.Equal(t, "API unavailable", snapshot.ProblemEvents[0].Message)
	<-done
}
