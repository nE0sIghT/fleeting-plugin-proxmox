package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectBalanced(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyBalanced))
	node, err := s.Select([]Node{
		{Name: "a", FreeMemoryMB: 2048, FreeDiskGB: 50, FreeCPUCores: 2},
		{Name: "b", FreeMemoryMB: 4096, FreeDiskGB: 50, FreeCPUCores: 4},
	}, Reserve{MemoryMB: 512, DiskGB: 10, CPUCores: 1}, Requirement{MemoryMB: 1024, DiskGB: 5, CPUCores: 1})
	require.NoError(t, err)
	require.Equal(t, "b", node.Name)
}

func TestSelectRejectsInsufficientHeadroom(t *testing.T) {
	t.Parallel()

	s := New(string(StrategyBalanced))
	_, err := s.Select([]Node{
		{Name: "a", FreeMemoryMB: 1024, FreeDiskGB: 20, FreeCPUCores: 1},
	}, Reserve{MemoryMB: 1024, DiskGB: 20, CPUCores: 1}, Requirement{MemoryMB: 512, DiskGB: 5, CPUCores: 1})
	require.Error(t, err)
}
