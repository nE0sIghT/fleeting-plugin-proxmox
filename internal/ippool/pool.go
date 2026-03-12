package ippool

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/state"
)

type Config struct {
	Prefix        netip.Prefix
	Gateway       netip.Addr
	Ranges        []string
	Exclude       []string
	ReuseCooldown time.Duration
}

type Lease struct {
	IP      netip.Addr
	Prefix  netip.Prefix
	Gateway netip.Addr
}

type Pool struct {
	cfg        Config
	store      state.Store
	candidates []netip.Addr
}

func New(cfg Config, store state.Store) (*Pool, error) {
	candidates, err := buildCandidates(cfg)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("ip pool has no allocatable addresses")
	}

	return &Pool{
		cfg:        cfg,
		store:      store,
		candidates: candidates,
	}, nil
}

func (p *Pool) Acquire(ctx context.Context, key string) (Lease, error) {
	if key == "" {
		return Lease{}, fmt.Errorf("empty lease key")
	}

	var out Lease
	err := p.store.Update(ctx, func(snapshot *state.Snapshot) error {
		now := time.Now()
		for ip, record := range snapshot.Leases {
			if record.Key != key {
				continue
			}
			addr, err := netip.ParseAddr(ip)
			if err != nil {
				return err
			}
			out = Lease{IP: addr, Prefix: p.cfg.Prefix, Gateway: p.cfg.Gateway}
			return nil
		}

		for _, candidate := range p.candidates {
			record, exists := snapshot.Leases[candidate.String()]
			if exists && record.Key != "" {
				continue
			}
			if exists && record.ReleasedAt != nil && record.ReleasedAt.Add(p.cfg.ReuseCooldown).After(now) {
				continue
			}

			snapshot.Leases[candidate.String()] = state.LeaseRecord{
				Key:         key,
				AllocatedAt: now,
			}
			out = Lease{IP: candidate, Prefix: p.cfg.Prefix, Gateway: p.cfg.Gateway}
			return nil
		}

		return fmt.Errorf("ip pool exhausted")
	})
	return out, err
}

func (p *Pool) Release(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}

	return p.store.Update(ctx, func(snapshot *state.Snapshot) error {
		now := time.Now()
		for ip, record := range snapshot.Leases {
			if record.Key != key {
				continue
			}
			record.Key = ""
			record.ReleasedAt = &now
			snapshot.Leases[ip] = record
		}
		return nil
	})
}

func (p *Pool) Forget(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}

	return p.store.Update(ctx, func(snapshot *state.Snapshot) error {
		for ip, record := range snapshot.Leases {
			if record.Key != key {
				continue
			}
			delete(snapshot.Leases, ip)
		}
		return nil
	})
}

func (p *Pool) Reconcile(ctx context.Context, active map[string]netip.Addr) error {
	return p.store.Update(ctx, func(snapshot *state.Snapshot) error {
		now := time.Now()
		for ip, record := range snapshot.Leases {
			if record.Key == "" {
				continue
			}
			if _, ok := active[record.Key]; ok {
				continue
			}
			record.Key = ""
			record.ReleasedAt = &now
			snapshot.Leases[ip] = record
		}

		for key, addr := range active {
			snapshot.Leases[addr.String()] = state.LeaseRecord{
				Key:         key,
				AllocatedAt: now,
			}
		}

		return nil
	})
}

func (p *Pool) Contains(addr netip.Addr) bool {
	return p.cfg.Prefix.Contains(addr)
}

func buildCandidates(cfg Config) ([]netip.Addr, error) {
	excluded := map[string]struct{}{}
	for _, value := range cfg.Exclude {
		addr, err := netip.ParseAddr(value)
		if err != nil {
			return nil, fmt.Errorf("parse excluded address %q: %w", value, err)
		}
		excluded[addr.String()] = struct{}{}
	}
	excluded[cfg.Gateway.String()] = struct{}{}

	var candidates []netip.Addr
	appendAddr := func(addr netip.Addr) {
		if !cfg.Prefix.Contains(addr) {
			return
		}
		if _, blocked := excluded[addr.String()]; blocked {
			return
		}
		candidates = append(candidates, addr)
	}

	if len(cfg.Ranges) == 0 {
		for addr := cfg.Prefix.Addr(); cfg.Prefix.Contains(addr); addr = addr.Next() {
			appendAddr(addr)
			if !addr.IsValid() {
				break
			}
		}
	} else {
		for _, value := range cfg.Ranges {
			start, end, err := parseRange(value)
			if err != nil {
				return nil, err
			}
			for addr := start; ; addr = addr.Next() {
				appendAddr(addr)
				if addr == end {
					break
				}
			}
		}
	}

	if len(candidates) > 0 {
		networkAddr := cfg.Prefix.Masked().Addr()
		broadcast := lastAddr(cfg.Prefix)
		candidates = slices.DeleteFunc(candidates, func(addr netip.Addr) bool {
			return addr == networkAddr || addr == broadcast
		})
	}

	return candidates, nil
}

func parseRange(value string) (netip.Addr, netip.Addr, error) {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("invalid IP range %q", value)
	}
	startAddr, err := netip.ParseAddr(parts[0])
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	endAddr, err := netip.ParseAddr(parts[1])
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	if startAddr.Compare(endAddr) > 0 {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("range start must be <= end")
	}
	return startAddr, endAddr, nil
}

func lastAddr(prefix netip.Prefix) netip.Addr {
	addr := prefix.Masked().Addr().As4()
	bits := prefix.Bits()
	hostBits := 32 - bits
	value := uint32(addr[0])<<24 | uint32(addr[1])<<16 | uint32(addr[2])<<8 | uint32(addr[3])
	value |= (1 << hostBits) - 1
	return netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
}
