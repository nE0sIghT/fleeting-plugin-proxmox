package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectBalanced(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyBalanced))
	node, err := s.Select([]Node{
		{Name: "a", TotalMemoryMB: 4096, FreeMemoryMB: 2048, TotalDiskGB: 100, FreeDiskGB: 50, TotalCPUCores: 4, FreeCPUCores: 2},
		{Name: "b", TotalMemoryMB: 8192, FreeMemoryMB: 4096, TotalDiskGB: 100, FreeDiskGB: 50, TotalCPUCores: 8, FreeCPUCores: 4},
	}, Reserve{MemoryMB: 512, DiskGB: 10, CPUCores: 1}, Requirement{MemoryMB: 1024, DiskGB: 5, CPUCores: 1})
	require.NoError(t, err)
	require.Equal(t, "b", node.Name)
}

func TestSelectRejectsInsufficientHeadroom(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyBalanced))
	_, err := s.Select([]Node{
		{Name: "a", TotalMemoryMB: 2048, FreeMemoryMB: 1024, TotalDiskGB: 40, FreeDiskGB: 20, TotalCPUCores: 2, FreeCPUCores: 1},
	}, Reserve{MemoryMB: 1024, DiskGB: 20, CPUCores: 1}, Requirement{MemoryMB: 512, DiskGB: 5, CPUCores: 1})
	require.Error(t, err)
}

func TestSelectUsesPercentReserve(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyBalanced))
	_, err := s.Select([]Node{
		{Name: "a", TotalMemoryMB: 64000, FreeMemoryMB: 7000, TotalDiskGB: 1000, FreeDiskGB: 150, TotalCPUCores: 64, FreeCPUCores: 12},
	}, Reserve{MemoryPercent: 10, DiskPercent: 20, CPUPercent: 25}, Requirement{MemoryMB: 1024, DiskGB: 10, CPUCores: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "memory")
	require.Contains(t, err.Error(), "disk")
	require.Contains(t, err.Error(), "cpu")
}

func TestSelectRejectsCPUAllocationLimit(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyMostFreeCPU))
	_, err := s.Select([]Node{
		{
			Name:                    "a",
			TotalMemoryMB:           64000,
			FreeMemoryMB:            64000,
			TotalDiskGB:             1000,
			FreeDiskGB:              1000,
			TotalCPUCores:           64,
			FreeCPUCores:            64,
			AllocatedCPUCores:       60,
			CPUAllocationLimitCores: 64,
		},
	}, Reserve{}, Requirement{MemoryMB: 1024, DiskGB: 10, CPUCores: 8})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cpu allocation")
}

func TestMostFreeCPUUsesAllocationHeadroom(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyMostFreeCPU))
	node, err := s.Select([]Node{
		{
			Name:                    "runtime-free",
			TotalMemoryMB:           64000,
			FreeMemoryMB:            64000,
			TotalDiskGB:             1000,
			FreeDiskGB:              1000,
			TotalCPUCores:           64,
			FreeCPUCores:            60,
			AllocatedCPUCores:       62,
			CPUAllocationLimitCores: 64,
		},
		{
			Name:                    "allocation-free",
			TotalMemoryMB:           64000,
			FreeMemoryMB:            64000,
			TotalDiskGB:             1000,
			FreeDiskGB:              1000,
			TotalCPUCores:           64,
			FreeCPUCores:            30,
			AllocatedCPUCores:       20,
			CPUAllocationLimitCores: 64,
		},
	}, Reserve{}, Requirement{MemoryMB: 1024, DiskGB: 10, CPUCores: 1})
	require.NoError(t, err)
	require.Equal(t, "allocation-free", node.Name)
}

func TestMostFreeRAMUsesAllocationHeadroom(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyMostFreeRAM))
	node, err := s.Select([]Node{
		{
			Name:                    "runtime-free",
			TotalMemoryMB:           64000,
			FreeMemoryMB:            60000,
			MemoryAllocationLimitMB: 64000,
			AllocatedMemoryMB:       63000,
			TotalDiskGB:             1000,
			FreeDiskGB:              1000,
			TotalCPUCores:           64,
			FreeCPUCores:            64,
		},
		{
			Name:                    "allocation-free",
			TotalMemoryMB:           64000,
			FreeMemoryMB:            30000,
			MemoryAllocationLimitMB: 64000,
			AllocatedMemoryMB:       20000,
			TotalDiskGB:             1000,
			FreeDiskGB:              1000,
			TotalCPUCores:           64,
			FreeCPUCores:            64,
		},
	}, Reserve{}, Requirement{MemoryMB: 512, DiskGB: 10, CPUCores: 1})
	require.NoError(t, err)
	require.Equal(t, "allocation-free", node.Name)
}
