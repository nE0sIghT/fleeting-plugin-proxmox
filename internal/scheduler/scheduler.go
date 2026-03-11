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
	MemoryMB      int64
	MemoryPercent int
	DiskGB        int64
	DiskPercent   int
	CPUCores      int
	CPUPercent    int
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
	TotalMemoryMB float64
	FreeMemoryMB  float64
	TotalDiskGB   float64
	FreeDiskGB    float64
	TotalCPUCores float64
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
		memoryReserve := reserve.memoryMBFor(node)
		diskReserve := reserve.diskGBFor(node)
		cpuReserve := reserve.cpuCoresFor(node)

		// Reserve is a placement guard against the node's current free capacity, not a
		// Proxmox reservation primitive. Nodes that would dip below the configured
		// headroom after placing one more VM are excluded.
		if node.FreeMemoryMB-requirement.MemoryMB < memoryReserve {
			continue
		}
		if node.FreeDiskGB-requirement.DiskGB < diskReserve {
			continue
		}
		if node.FreeCPUCores-requirement.CPUCores < cpuReserve {
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
		memoryReserve := reserve.memoryMBFor(node)
		diskReserve := reserve.diskGBFor(node)
		cpuReserve := reserve.cpuCoresFor(node)
		if node.FreeMemoryMB-requirement.MemoryMB < memoryReserve {
			parts = append(parts, fmt.Sprintf("memory free=%.0fMB need=%.0fMB reserve=%.0fMB", node.FreeMemoryMB, requirement.MemoryMB, memoryReserve))
		}
		if node.FreeDiskGB-requirement.DiskGB < diskReserve {
			storage := node.TargetStorage
			if storage == "" {
				storage = "<default>"
			}
			parts = append(parts, fmt.Sprintf("disk[%s] free=%.1fGB need=%.1fGB reserve=%.1fGB", storage, node.FreeDiskGB, requirement.DiskGB, diskReserve))
		}
		if node.FreeCPUCores-requirement.CPUCores < cpuReserve {
			parts = append(parts, fmt.Sprintf("cpu free=%.2f need=%.2f reserve=%.2f", node.FreeCPUCores, requirement.CPUCores, cpuReserve))
		}
		if len(parts) == 0 {
			continue
		}
		reasons = append(reasons, fmt.Sprintf("%s: %s", node.Name, strings.Join(parts, ", ")))
	}
	return reasons
}

func (r Reserve) memoryMBFor(node Node) float64 {
	if r.MemoryPercent > 0 && node.TotalMemoryMB > 0 {
		return node.TotalMemoryMB * float64(r.MemoryPercent) / 100.0
	}
	return float64(r.MemoryMB)
}

func (r Reserve) diskGBFor(node Node) float64 {
	if r.DiskPercent > 0 && node.TotalDiskGB > 0 {
		return node.TotalDiskGB * float64(r.DiskPercent) / 100.0
	}
	return float64(r.DiskGB)
}

func (r Reserve) cpuCoresFor(node Node) float64 {
	if r.CPUPercent > 0 && node.TotalCPUCores > 0 {
		return node.TotalCPUCores * float64(r.CPUPercent) / 100.0
	}
	return float64(r.CPUCores)
}
