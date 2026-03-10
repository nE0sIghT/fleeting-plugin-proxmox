package ippool

import (
	"context"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/state"
)

func TestPoolAcquireReleaseReconcile(t *testing.T) {
	t.Parallel()

	store := state.NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	pool, err := New(Config{
		Prefix:        netip.MustParsePrefix("10.0.0.0/29"),
		Gateway:       netip.MustParseAddr("10.0.0.1"),
		Ranges:        []string{"10.0.0.2-10.0.0.6"},
		ReuseCooldown: 0,
	}, store)
	require.NoError(t, err)

	ctx := context.Background()

	leaseA, err := pool.Acquire(ctx, "node1/100")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2", leaseA.IP.String())

	leaseB, err := pool.Acquire(ctx, "node1/101")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.3", leaseB.IP.String())

	require.NoError(t, pool.Release(ctx, "node1/100"))

	leaseC, err := pool.Acquire(ctx, "node1/102")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2", leaseC.IP.String())

	require.NoError(t, pool.Reconcile(ctx, map[string]netip.Addr{
		"node1/101": netip.MustParseAddr("10.0.0.3"),
	}))

	snapshot, err := store.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "node1/101", snapshot.Leases["10.0.0.3"].Key)
	require.Empty(t, snapshot.Leases["10.0.0.2"].Key)
	require.NotNil(t, snapshot.Leases["10.0.0.2"].ReleasedAt)
}

func TestPoolRespectsReuseCooldown(t *testing.T) {
	t.Parallel()

	store := state.NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	pool, err := New(Config{
		Prefix:        netip.MustParsePrefix("10.0.1.0/30"),
		Gateway:       netip.MustParseAddr("10.0.1.1"),
		Ranges:        []string{"10.0.1.2-10.0.1.2"},
		ReuseCooldown: time.Hour,
	}, store)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = pool.Acquire(ctx, "node1/100")
	require.NoError(t, err)
	require.NoError(t, pool.Release(ctx, "node1/100"))

	_, err = pool.Acquire(ctx, "node1/101")
	require.Error(t, err)
}
