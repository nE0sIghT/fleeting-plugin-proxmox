package metrics

import (
	"errors"
	"fmt"
	"sync"
)

var ErrPlacementPaused = errors.New("placement paused")

// PlacementControl serializes drain application with clone submission, not with
// the entire Proxmox task. A submitted clone is allowed to finish.
type PlacementControl struct {
	mu      sync.RWMutex
	enabled bool
	ready   bool
	drained map[string]bool
}

func (p *PlacementControl) Enable() { p.mu.Lock(); p.enabled = true; p.mu.Unlock() }

func (p *PlacementControl) Apply(state DrainState, cluster string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drained = map[string]bool{}
	for _, node := range state.Nodes {
		if node.Cluster == cluster {
			p.drained[node.Node] = true
		}
	}
	p.ready = true
}

func (p *PlacementControl) rejection(node string) error {
	if !p.enabled {
		return nil
	}
	if !p.ready {
		return fmt.Errorf("%w: waiting for initial synchronization", ErrPlacementPaused)
	}
	if p.drained[node] {
		return fmt.Errorf("%w: node %s is drained", ErrPlacementPaused, node)
	}
	return nil
}

func (p *PlacementControl) Check(node string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.rejection(node)
}

func (p *PlacementControl) BeginClone(node string) (func(), error) {
	p.mu.RLock()
	if err := p.rejection(node); err != nil {
		p.mu.RUnlock()
		return nil, err
	}
	return p.mu.RUnlock, nil
}
