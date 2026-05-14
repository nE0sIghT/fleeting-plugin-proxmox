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

func TestPoolForgetRemovesLeaseImmediately(t *testing.T) {
	t.Parallel()

	store := state.NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	pool, err := New(Config{
		Prefix:        netip.MustParsePrefix("10.0.2.0/30"),
		Gateway:       netip.MustParseAddr("10.0.2.1"),
		Ranges:        []string{"10.0.2.2-10.0.2.2"},
		ReuseCooldown: time.Hour,
	}, store)
	require.NoError(t, err)

	ctx := context.Background()
	lease, err := pool.Acquire(ctx, "node1/100")
	require.NoError(t, err)
	require.Equal(t, "10.0.2.2", lease.IP.String())

	require.NoError(t, pool.Forget(ctx, "node1/100"))

	snapshot, err := store.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, snapshot.Leases)

	lease, err = pool.Acquire(ctx, "node1/101")
	require.NoError(t, err)
	require.Equal(t, "10.0.2.2", lease.IP.String())
}

func TestPoolAcquireIgnoresExistingLeaseOutsideCurrentPool(t *testing.T) {
	t.Parallel()

	store := state.NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	oldPool, err := New(Config{
		Prefix:        netip.MustParsePrefix("10.0.3.0/30"),
		Gateway:       netip.MustParseAddr("10.0.3.1"),
		Ranges:        []string{"10.0.3.2-10.0.3.2"},
		ReuseCooldown: 0,
	}, store)
	require.NoError(t, err)

	ctx := context.Background()
	lease, err := oldPool.Acquire(ctx, "node1/100")
	require.NoError(t, err)
	require.Equal(t, "10.0.3.2", lease.IP.String())

	newPool, err := New(Config{
		Prefix:        netip.MustParsePrefix("10.0.4.0/30"),
		Gateway:       netip.MustParseAddr("10.0.4.1"),
		Ranges:        []string{"10.0.4.2-10.0.4.2"},
		ReuseCooldown: 0,
	}, store)
	require.NoError(t, err)

	lease, err = newPool.Acquire(ctx, "node1/100")
	require.NoError(t, err)
	require.Equal(t, "10.0.4.2", lease.IP.String())

	snapshot, err := store.Read(ctx)
	require.NoError(t, err)
	require.NotContains(t, snapshot.Leases, "10.0.3.2")
	require.Equal(t, "node1/100", snapshot.Leases["10.0.4.2"].Key)
}

func TestPoolReconcilePrunesAddressesOutsideCurrentPool(t *testing.T) {
	t.Parallel()

	store := state.NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	pool, err := New(Config{
		Prefix:        netip.MustParsePrefix("10.0.5.0/29"),
		Gateway:       netip.MustParseAddr("10.0.5.1"),
		Ranges:        []string{"10.0.5.2-10.0.5.3"},
		Exclude:       []string{"10.0.5.3"},
		ReuseCooldown: 0,
	}, store)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, pool.Reconcile(ctx, map[string]netip.Addr{
		"node1/100": netip.MustParseAddr("10.0.5.2"),
		"node1/101": netip.MustParseAddr("10.0.5.3"),
		"node1/102": netip.MustParseAddr("10.0.6.2"),
	}))

	snapshot, err := store.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "node1/100", snapshot.Leases["10.0.5.2"].Key)
	require.NotContains(t, snapshot.Leases, "10.0.5.3")
	require.NotContains(t, snapshot.Leases, "10.0.6.2")
	require.True(t, pool.Allows(netip.MustParseAddr("10.0.5.2")))
	require.False(t, pool.Allows(netip.MustParseAddr("10.0.5.3")))
	require.False(t, pool.Allows(netip.MustParseAddr("10.0.6.2")))
}
