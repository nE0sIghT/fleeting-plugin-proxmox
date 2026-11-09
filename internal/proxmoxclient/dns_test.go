package proxmoxclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testDNSDialer(t *testing.T) *dnsDialer {
	t.Helper()
	// An IP host avoids starting the real five-minute background timer in unit tests.
	d := newDNSDialer("127.0.0.1", nil, nil)
	d.host = "cluster.example.test"
	t.Cleanup(d.close)
	return d
}

func expireDNSLookup(d *dnsDialer) {
	d.mu.Lock()
	d.lastLookup = time.Now().Add(-dnsRefreshInterval)
	d.mu.Unlock()
}

func TestDNSCacheRetainsAddressesOnInvalidAnswer(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"NXDOMAIN", &net.DNSError{Name: "cluster.example.test", Err: "no such host", IsNotFound: true}},
		{"temporary failure", errors.New("DNS unavailable")},
		{"empty answer", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testDNSDialer(t)
			calls := 0
			d.lookup = func(context.Context, string) ([]net.IPAddr, error) {
				calls++
				if calls == 1 {
					return []net.IPAddr{{IP: net.ParseIP("192.0.2.1")}}, nil
				}
				return nil, tc.err
			}
			require.NoError(t, d.refresh(context.Background()))
			expireDNSLookup(d)
			require.Error(t, d.refresh(context.Background()))
			require.Equal(t, []string{"192.0.2.1"}, d.candidates())
			require.Error(t, d.refresh(context.Background()))
			require.Equal(t, 2, calls, "failed DNS lookups must be rate limited")
		})
	}
}

func TestDNSCacheReplacesAndDeduplicatesAddresses(t *testing.T) {
	d := testDNSDialer(t)
	addresses := []net.IPAddr{{IP: net.ParseIP("192.0.2.1")}}
	d.lookup = func(context.Context, string) ([]net.IPAddr, error) { return addresses, nil }
	require.NoError(t, d.refresh(context.Background()))
	d.recordResult("192.0.2.1", errors.New("offline"))
	addresses = []net.IPAddr{
		{IP: net.ParseIP("2001:db8::1")}, {IP: net.ParseIP("192.0.2.2")}, {IP: net.ParseIP("192.0.2.2")},
	}
	expireDNSLookup(d)
	require.NoError(t, d.refresh(context.Background()))
	require.ElementsMatch(t, []string{"192.0.2.2", "2001:db8::1"}, d.candidates())
	require.Empty(t, d.failedUntil)
}

func TestDNSConcurrentInitialLookup(t *testing.T) {
	d := testDNSDialer(t)
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	d.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.1")}}, nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- d.refresh(context.Background())
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestDNSCanceledInitialLookupCanBeRetried(t *testing.T) {
	d := testDNSDialer(t)
	d.lookup = func(ctx context.Context, _ string) ([]net.IPAddr, error) { return nil, ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, d.refresh(ctx), context.Canceled)
	d.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.1")}}, nil
	}
	require.NoError(t, d.refresh(context.Background()))
}

func TestDNSBackgroundRefreshAndShutdown(t *testing.T) {
	d := testDNSDialer(t)
	lookedUp := make(chan struct{}, 1)
	d.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		lookedUp <- struct{}{}
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.1")}}, nil
	}
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.refreshLoop(ticks)
	}()
	ticks <- time.Now()
	select {
	case <-lookedUp:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not resolve DNS")
	}
	d.close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background resolver did not stop")
	}
}

func TestDNSDialFailoverCooldownAndRecovery(t *testing.T) {
	d := testDNSDialer(t)
	d.addresses = []string{"192.0.2.1", "192.0.2.2"}
	d.lastLookup = time.Now()
	var tried []string
	offline := true
	d.dial = func(_ context.Context, _, address string) (net.Conn, error) {
		tried = append(tried, address)
		if offline && address == "192.0.2.1:8006" {
			return nil, errors.New("connection refused")
		}
		conn, peer := net.Pipe()
		_ = peer.Close()
		return conn, nil
	}
	conn, err := d.DialContext(context.Background(), "tcp", "cluster.example.test:8006")
	require.NoError(t, err)
	_ = conn.Close()
	require.Equal(t, []string{"192.0.2.1:8006", "192.0.2.2:8006"}, tried)
	require.Equal(t, []string{"192.0.2.2"}, d.candidates())
	offline = false
	d.failedUntil["192.0.2.1"] = time.Now().Add(-time.Second)
	for range 2 {
		conn, err = d.DialContext(context.Background(), "tcp", "cluster.example.test:8006")
		require.NoError(t, err)
		_ = conn.Close()
	}
	require.Empty(t, d.failedUntil)
}

func TestDNSDialRefreshesAfterAllAddressesFail(t *testing.T) {
	d := testDNSDialer(t)
	d.addresses = []string{"192.0.2.1"}
	d.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.2")}}, nil
	}
	d.dial = func(_ context.Context, _, address string) (net.Conn, error) {
		if address == "192.0.2.1:8006" {
			return nil, errors.New("connection refused")
		}
		conn, peer := net.Pipe()
		_ = peer.Close()
		return conn, nil
	}
	conn, err := d.DialContext(context.Background(), "tcp", "cluster.example.test:8006")
	require.NoError(t, err)
	_ = conn.Close()
	require.Equal(t, []string{"192.0.2.2"}, d.candidates())
}

func TestDNSDialSharesBudgetAmongAddresses(t *testing.T) {
	d := testDNSDialer(t)
	d.addresses = []string{"192.0.2.1", "192.0.2.2"}
	d.lastLookup = time.Now()
	calls := 0
	d.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		calls++
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), dialAttemptTimeout)
		if calls == 1 {
			require.LessOrEqual(t, time.Until(deadline), 500*time.Millisecond)
			return nil, context.DeadlineExceeded
		}
		conn, peer := net.Pipe()
		_ = peer.Close()
		return conn, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := d.DialContext(ctx, "tcp", "cluster.example.test:8006")
	require.NoError(t, err)
	_ = conn.Close()
	require.Equal(t, 2, calls, "the first IP must not consume the entire connection budget")
}

func TestDNSDialHonorsCancellationWithoutMarkingAddressFailed(t *testing.T) {
	d := testDNSDialer(t)
	d.addresses = []string{"192.0.2.1", "192.0.2.2"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	d.dial = func(context.Context, string, string) (net.Conn, error) {
		calls++
		cancel()
		return nil, context.Canceled
	}
	_, err := d.DialContext(ctx, "tcp", "cluster.example.test:8006")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	require.Empty(t, d.failedUntil)
}

func TestDNSHTTPSPreservesHostAndSNIWithoutRepeatingPost(t *testing.T) {
	const host = "cluster.example.test"
	cert, caFile := newTestServerCertificate(t, host)
	var posts atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHost, _, err := net.SplitHostPort(r.Host)
		if err != nil || r.TLS.ServerName != host || requestHost != host {
			t.Errorf("incorrect HTTPS identity: SNI=%q Host=%q", r.TLS.ServerName, r.Host)
		}
		if r.Method == http.MethodPost {
			posts.Add(1)
			_, _ = fmt.Fprint(w, `{"data":"UPID:clone"}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":{"release":"8.2"}}`)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	defer server.Close()
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	port := u.Port()
	u.Host = net.JoinHostPort(host, port)
	client, err := New(Config{BaseURL: u.String(), TLSCAFile: caFile})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	client.dns.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.2")}, {IP: net.ParseIP("127.0.0.1")}}, nil
	}
	// Select the refused address first, then establish HTTPS on the live address.
	client.dns.next = 1
	_, err = client.CloneVM(context.Background(), "node1", 100, CloneRequest{NewID: 101})
	require.NoError(t, err)
	require.Equal(t, int32(1), posts.Load())
	require.Contains(t, client.dns.failedUntil, "127.0.0.2")
}
