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

type NodePolicy struct {
	Reserve                      Reserve
	MemoryAllocationLimitPercent int
	CPUAllocationLimitPercent    int
}

type Requirement struct {
	MemoryMB float64
	DiskGB   float64
	CPUCores float64
}

type Node struct {
	Name                    string
	TemplateNode            string
	TemplateVMID            int
	TargetStorage           string
	TotalMemoryMB           float64
	FreeMemoryMB            float64
	AllocatedMemoryMB       float64
	MemoryAllocationLimitMB float64
	TotalDiskGB             float64
	FreeDiskGB              float64
	TotalCPUCores           float64
	FreeCPUCores            float64
	AllocatedCPUCores       float64
	CPUAllocationLimitCores float64
	Reserve                 Reserve
	ReserveSet              bool
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
		return "no eligible nodes satisfy configured placement constraints"
	}
	return fmt.Sprintf("no eligible nodes satisfy configured placement constraints: %s", strings.Join(e.Reasons, "; "))
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
		if len(admissionFailures(node, reserveFor(node, reserve), requirement)) > 0 {
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
			scoreA := effectiveFreeMemoryMB(a, reserveFor(a, reserve))
			scoreB := effectiveFreeMemoryMB(b, reserveFor(b, reserve))
			return compareScore(scoreA, scoreB)
		})
	case StrategyMostFreeCPU:
		slices.SortStableFunc(eligible, func(a, b Node) int {
			scoreA := effectiveFreeCPUCores(a, reserveFor(a, reserve))
			scoreB := effectiveFreeCPUCores(b, reserveFor(b, reserve))
			return compareScore(scoreA, scoreB)
		})
	case StrategyRoundRobin:
		idx := s.next % len(eligible)
		s.next++
		return eligible[idx], nil
	default:
		slices.SortStableFunc(eligible, func(a, b Node) int {
			scoreA := balancedScore(a, reserve)
			scoreB := balancedScore(b, reserve)
			return compareScore(scoreA, scoreB)
		})
	}

	return eligible[0], nil
}

func Diagnose(nodes []Node, reserve Reserve, requirement Requirement) []string {
	reasons := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts := admissionFailures(node, reserveFor(node, reserve), requirement)
		if len(parts) == 0 {
			continue
		}
		reasons = append(reasons, fmt.Sprintf("%s: %s", node.Name, strings.Join(parts, ", ")))
	}
	return reasons
}

func balancedScore(node Node, fallback Reserve) float64 {
	reserve := reserveFor(node, fallback)
	return effectiveFreeMemoryMB(node, reserve) +
		(effectiveFreeDiskGB(node, reserve) * 10) +
		(effectiveFreeCPUCores(node, reserve) * 1024)
}

func compareScore(a, b float64) int {
	switch {
	case a > b:
		return -1
	case a < b:
		return 1
	default:
		return 0
	}
}

func admissionFailures(node Node, reserve Reserve, requirement Requirement) []string {
	var parts []string
	memoryReserve := reserve.memoryMBFor(node)
	diskReserve := reserve.diskGBFor(node)
	cpuReserve := reserve.cpuCoresFor(node)

	// Reserve is a placement guard against the node's current free capacity, not a
	// Proxmox reservation primitive. Nodes that would dip below the configured
	// headroom after placing one more VM are excluded.
	if node.FreeMemoryMB-requirement.MemoryMB < memoryReserve {
		parts = append(parts, fmt.Sprintf("memory free=%.0fMB need=%.0fMB reserve=%.0fMB", node.FreeMemoryMB, requirement.MemoryMB, memoryReserve))
	}
	if node.MemoryAllocationLimitMB > 0 && node.MemoryAllocationLimitMB-node.AllocatedMemoryMB < requirement.MemoryMB {
		parts = append(parts, fmt.Sprintf("memory allocation allocated=%.0fMB need=%.0fMB limit=%.0fMB", node.AllocatedMemoryMB, requirement.MemoryMB, node.MemoryAllocationLimitMB))
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
	if node.CPUAllocationLimitCores > 0 && node.CPUAllocationLimitCores-node.AllocatedCPUCores < requirement.CPUCores {
		parts = append(parts, fmt.Sprintf("cpu allocation allocated=%.2f need=%.2f limit=%.2f", node.AllocatedCPUCores, requirement.CPUCores, node.CPUAllocationLimitCores))
	}
	return parts
}

func effectiveFreeMemoryMB(node Node, reserve Reserve) float64 {
	free := node.FreeMemoryMB - reserve.memoryMBFor(node)
	physicalFree := physicalFreeMemoryMB(node)
	if physicalFree < free {
		free = physicalFree
	}
	return free
}

func effectiveFreeDiskGB(node Node, reserve Reserve) float64 {
	return node.FreeDiskGB - reserve.diskGBFor(node)
}

func effectiveFreeCPUCores(node Node, reserve Reserve) float64 {
	free := node.FreeCPUCores - reserve.cpuCoresFor(node)
	physicalFree := physicalFreeCPUCores(node)
	if physicalFree < free {
		free = physicalFree
	}
	return free
}

func physicalFreeMemoryMB(node Node) float64 {
	if node.TotalMemoryMB <= 0 {
		return node.FreeMemoryMB
	}
	free := node.TotalMemoryMB - node.AllocatedMemoryMB
	if free < 0 {
		return 0
	}
	return free
}

func physicalFreeCPUCores(node Node) float64 {
	if node.TotalCPUCores <= 0 {
		return node.FreeCPUCores
	}
	free := node.TotalCPUCores - node.AllocatedCPUCores
	if free < 0 {
		return 0
	}
	return free
}

func reserveFor(node Node, fallback Reserve) Reserve {
	if node.ReserveSet {
		return node.Reserve
	}
	return fallback
}

func (r Reserve) MemoryMBFor(node Node) float64 {
	return r.memoryMBFor(node)
}

func (r Reserve) DiskGBFor(node Node) float64 {
	return r.diskGBFor(node)
}

func (r Reserve) CPUCoresFor(node Node) float64 {
	return r.cpuCoresFor(node)
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
