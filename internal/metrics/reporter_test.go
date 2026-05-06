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
