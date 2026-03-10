package scheduler

import (
	"fmt"
	"slices"
)

type Strategy string

const (
	StrategyBalanced    Strategy = "balanced"
	StrategyMostFreeRAM Strategy = "most_free_ram"
	StrategyMostFreeCPU Strategy = "most_free_cpu"
	StrategyRoundRobin  Strategy = "round_robin"
)

type Reserve struct {
	MemoryMB int64
	DiskGB   int64
	CPUCores int
}

type Requirement struct {
	MemoryMB float64
	DiskGB   float64
	CPUCores float64
}

type Node struct {
	Name         string
	FreeMemoryMB float64
	FreeDiskGB   float64
	FreeCPUCores float64
}

type Scheduler struct {
	strategy Strategy
	next     int
}

func New(strategy string) *Scheduler {
	if strategy == "" {
		strategy = string(StrategyBalanced)
	}
	return &Scheduler{strategy: Strategy(strategy)}
}

func (s *Scheduler) Select(nodes []Node, reserve Reserve, requirement Requirement) (Node, error) {
	eligible := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		// Reserve is a placement guard against the node's current free capacity, not a
		// Proxmox reservation primitive. Nodes that would dip below the configured
		// headroom after placing one more VM are excluded.
		if node.FreeMemoryMB-requirement.MemoryMB < float64(reserve.MemoryMB) {
			continue
		}
		if node.FreeDiskGB-requirement.DiskGB < float64(reserve.DiskGB) {
			continue
		}
		if node.FreeCPUCores-requirement.CPUCores < float64(reserve.CPUCores) {
			continue
		}
		eligible = append(eligible, node)
	}

	if len(eligible) == 0 {
		return Node{}, fmt.Errorf("no eligible nodes satisfy configured headroom")
	}

	switch s.strategy {
	case StrategyMostFreeRAM:
		slices.SortStableFunc(eligible, func(a, b Node) int {
			switch {
			case a.FreeMemoryMB > b.FreeMemoryMB:
				return -1
			case a.FreeMemoryMB < b.FreeMemoryMB:
				return 1
			default:
				return 0
			}
		})
	case StrategyMostFreeCPU:
		slices.SortStableFunc(eligible, func(a, b Node) int {
			switch {
			case a.FreeCPUCores > b.FreeCPUCores:
				return -1
			case a.FreeCPUCores < b.FreeCPUCores:
				return 1
			default:
				return 0
			}
		})
	case StrategyRoundRobin:
		idx := s.next % len(eligible)
		s.next++
		return eligible[idx], nil
	default:
		slices.SortStableFunc(eligible, func(a, b Node) int {
			scoreA := a.FreeMemoryMB + (a.FreeDiskGB * 10) + (a.FreeCPUCores * 1024)
			scoreB := b.FreeMemoryMB + (b.FreeDiskGB * 10) + (b.FreeCPUCores * 1024)
			switch {
			case scoreA > scoreB:
				return -1
			case scoreA < scoreB:
				return 1
			default:
				return 0
			}
		})
	}

	return eligible[0], nil
}
