package scheduler

import (
	"fmt"
	"slices"
	"strings"
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
	Name          string
	TemplateNode  string
	TemplateVMID  int
	TargetStorage string
	FreeMemoryMB  float64
	FreeDiskGB    float64
	FreeCPUCores  float64
}

type Scheduler struct {
	strategy Strategy
	next     int
}

type PlacementError struct {
	Reasons []string
}

func (e *PlacementError) Error() string {
	if len(e.Reasons) == 0 {
		return "no eligible nodes satisfy configured headroom"
	}
	return fmt.Sprintf("no eligible nodes satisfy configured headroom: %s", strings.Join(e.Reasons, "; "))
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
		return Node{}, &PlacementError{Reasons: Diagnose(nodes, reserve, requirement)}
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

func Diagnose(nodes []Node, reserve Reserve, requirement Requirement) []string {
	reasons := make([]string, 0, len(nodes))
	for _, node := range nodes {
		var parts []string
		if node.FreeMemoryMB-requirement.MemoryMB < float64(reserve.MemoryMB) {
			parts = append(parts, fmt.Sprintf("memory free=%.0fMB need=%.0fMB reserve=%dMB", node.FreeMemoryMB, requirement.MemoryMB, reserve.MemoryMB))
		}
		if node.FreeDiskGB-requirement.DiskGB < float64(reserve.DiskGB) {
			storage := node.TargetStorage
			if storage == "" {
				storage = "<default>"
			}
			parts = append(parts, fmt.Sprintf("disk[%s] free=%.1fGB need=%.1fGB reserve=%dGB", storage, node.FreeDiskGB, requirement.DiskGB, reserve.DiskGB))
		}
		if node.FreeCPUCores-requirement.CPUCores < float64(reserve.CPUCores) {
			parts = append(parts, fmt.Sprintf("cpu free=%.2f need=%.2f reserve=%d", node.FreeCPUCores, requirement.CPUCores, reserve.CPUCores))
		}
		if len(parts) == 0 {
			continue
		}
		reasons = append(reasons, fmt.Sprintf("%s: %s", node.Name, strings.Join(parts, ", ")))
	}
	return reasons
}
