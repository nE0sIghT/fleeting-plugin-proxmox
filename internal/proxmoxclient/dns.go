package proxmoxclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
)

const (
	dnsRefreshInterval = 5 * time.Minute
	dnsRetryInterval   = 30 * time.Second
	dnsLookupTimeout   = 5 * time.Second
	dialTimeout        = 15 * time.Second
	dialAttemptTimeout = 3 * time.Second
	addressCooldown    = 30 * time.Second
)

type dnsDialer struct {
	host   string
	lookup func(context.Context, string) ([]net.IPAddr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
	info   func(string, ...any)
	warn   func(string, ...any)

	mu          sync.Mutex
	addresses   []string
	failedUntil map[string]time.Time
	lastLookup  time.Time
	lookupErr   error
	refreshing  chan struct{}
	next        uint64
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func newDNSDialer(host string, info, warn func(string, ...any)) *dnsDialer {
	ctx, cancel := context.WithCancel(context.Background())
	d := &dnsDialer{
		host: host, lookup: net.DefaultResolver.LookupIPAddr,
		dial: (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext,
		info: info, warn: warn, failedUntil: make(map[string]time.Time),
		ctx: ctx, cancel: cancel,
	}
	if net.ParseIP(host) == nil && host != "" {
		d.startRefresh()
	}
	return d
}

func (d *dnsDialer) close() {
	d.cancel()
	d.wg.Wait()
}

func (d *dnsDialer) startRefresh() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		ticker := time.NewTicker(dnsRefreshInterval)
		defer ticker.Stop()
		d.refreshLoop(ticker.C)
	}()
}

func (d *dnsDialer) refreshLoop(ticks <-chan time.Time) {
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticks:
			_ = d.refresh(d.ctx)
		}
	}
}

func (d *dnsDialer) refresh(ctx context.Context) error {
	d.mu.Lock()
	if done := d.refreshing; done != nil {
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			d.mu.Lock()
			err := d.lookupErr
			d.mu.Unlock()
			return err
		}
	}
	if !d.lastLookup.IsZero() && time.Since(d.lastLookup) < dnsRetryInterval {
		err := d.lookupErr
		d.mu.Unlock()
		return err
	}
	d.refreshing = make(chan struct{})
	d.lastLookup = time.Now()
	d.mu.Unlock()

	lookupCtx, cancel := context.WithTimeout(ctx, dnsLookupTimeout)
	ips, err := d.lookup(lookupCtx, d.host)
	cancel()
	addresses := make([]string, 0, len(ips))
	for _, ip := range ips {
		if ip.IP == nil {
			continue
		}
		address := ip.String()
		if !slices.Contains(addresses, address) {
			addresses = append(addresses, address)
		}
	}
	if err == nil && len(addresses) == 0 {
		err = fmt.Errorf("DNS returned no addresses for %s", d.host)
	}
	// DNS answer order is not a topology change.
	slices.Sort(addresses)
	d.mu.Lock()
	changed := err == nil && !slices.Equal(d.addresses, addresses)
	if err == nil {
		d.addresses = addresses
		for address := range d.failedUntil {
			if !slices.Contains(addresses, address) {
				delete(d.failedUntil, address)
			}
		}
	}
	d.lookupErr = err
	if ctx.Err() != nil {
		// A canceled request must not suppress the next caller's initial lookup.
		d.lastLookup = time.Time{}
	}
	close(d.refreshing)
	d.refreshing = nil
	d.mu.Unlock()
	if err != nil && d.warn != nil && ctx.Err() == nil {
		d.warn("DNS refresh failed; retaining cached addresses", "api_host", d.host, "error", err)
	} else if changed && d.info != nil {
		d.info("API DNS addresses updated", "api_host", d.host, "addresses", addresses)
	}
	return err
}

func (d *dnsDialer) candidates() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var available []string
	for _, address := range d.addresses {
		if !time.Now().Before(d.failedUntil[address]) {
			available = append(available, address)
		}
	}
	// If every address is cooling down, allow a recovery probe without waiting.
	if len(available) == 0 {
		available = slices.Clone(d.addresses)
	}
	if len(available) > 0 {
		start := int(d.next % uint64(len(available)))
		d.next++
		available = append(available[start:], available[:start]...)
	}
	return available
}

func (d *dnsDialer) recordResult(address string, err error) {
	d.mu.Lock()
	_, failed := d.failedUntil[address]
	if err == nil {
		delete(d.failedUntil, address)
	} else {
		d.failedUntil[address] = time.Now().Add(addressCooldown)
	}
	d.mu.Unlock()
	if err != nil && !failed && d.warn != nil {
		d.warn("API TCP address unavailable", "api_host", d.host, "address", address, "error", err)
	} else if err == nil && failed && d.info != nil {
		d.info("API TCP address recovered", "api_host", d.host, "address", address)
	}
}

func (d *dnsDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if host != d.host || net.ParseIP(host) != nil {
		return d.dial(ctx, network, address)
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	candidates := d.candidates()
	if len(candidates) == 0 {
		if err := d.refresh(ctx); err != nil {
			return nil, fmt.Errorf("resolve API host %s: %w", host, err)
		}
		candidates = d.candidates()
	}
	tried := make(map[string]bool)
	var errs []error
	for round := 0; round < 2; round++ {
		candidates = slices.DeleteFunc(candidates, func(ip string) bool { return tried[ip] })
		for i, ip := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			tried[ip] = true
			deadline, _ := ctx.Deadline()
			// Share the remaining budget so a large answer cannot starve its last IPs.
			timeout := min(dialAttemptTimeout, time.Until(deadline)/time.Duration(len(candidates)-i))
			attemptCtx, attemptCancel := context.WithTimeout(ctx, timeout)
			conn, err := d.dial(attemptCtx, network, net.JoinHostPort(ip, port))
			attemptCancel()
			if ctx.Err() != nil && err != nil {
				return nil, ctx.Err()
			}
			d.recordResult(ip, err)
			if err == nil {
				return conn, nil
			}
			errs = append(errs, fmt.Errorf("connect API address %s: %w", ip, err))
		}
		if round == 0 {
			if err := d.refresh(ctx); err != nil {
				errs = append(errs, err)
			}
			candidates = d.candidates()
		}
	}
	return nil, fmt.Errorf("no reachable API address for %s: %w", host, errors.Join(errs...))
}
