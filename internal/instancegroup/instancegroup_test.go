package instancegroup

import (
	"testing"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/scheduler"
)

func TestReservePlannedResourcesSpreadsAcrossNodes(t *testing.T) {
	t.Parallel()

	group := &Group{
		cfg: Config{
			TargetStorages: []string{"fast-a", "fast-b"},
			Scheduler:      scheduler.New(string(scheduler.StrategyBalanced)),
		},
	}

	states := []nodePlanState{
		{
			Name:         "node1",
			FreeMemoryMB: 4096,
			FreeCPUCores: 4,
			StorageFreeGB: map[string]float64{
				"fast-a": 100,
			},
		},
		{
			Name:         "node2",
			FreeMemoryMB: 4096,
			FreeCPUCores: 4,
			StorageFreeGB: map[string]float64{
				"fast-b": 100,
			},
		},
	}

	req := scheduler.Requirement{MemoryMB: 3072, CPUCores: 2, DiskGB: 20}

	candidates, skipped := group.buildCandidateNodes(states)
	require.Empty(t, skipped)
	first, err := group.cfg.Scheduler.Select(candidates, scheduler.Reserve{}, req)
	require.NoError(t, err)
	group.reservePlannedResources(states, first, req)

	candidates, skipped = group.buildCandidateNodes(states)
	require.Empty(t, skipped)
	second, err := group.cfg.Scheduler.Select(candidates, scheduler.Reserve{}, req)
	require.NoError(t, err)

	require.NotEqual(t, first.Name, second.Name)
}

func TestPendingReservationsAffectNextPlanningPass(t *testing.T) {
	t.Parallel()

	group := &Group{
		cfg: Config{
			TargetStorages: []string{"fast-a", "fast-b"},
			Scheduler:      scheduler.New(string(scheduler.StrategyBalanced)),
		},
		pendingByNode: map[string]pendingReservation{
			"node1": {
				MemoryMB: 3072,
				CPUCores: 2,
				StorageGB: map[string]float64{
					"fast-a": 20,
				},
			},
		},
		pendingVMIDs: map[int]struct{}{5000: {}},
	}

	states := []nodePlanState{
		{
			Name:          "node1",
			TotalMemoryMB: 8192,
			FreeMemoryMB:  4096,
			TotalCPUCores: 4,
			FreeCPUCores:  4,
			StorageTotalGB: map[string]float64{
				"fast-a": 100,
			},
			StorageFreeGB: map[string]float64{
				"fast-a": 100,
			},
		},
		{
			Name:          "node2",
			TotalMemoryMB: 8192,
			FreeMemoryMB:  4096,
			TotalCPUCores: 4,
			FreeCPUCores:  4,
			StorageTotalGB: map[string]float64{
				"fast-b": 100,
			},
			StorageFreeGB: map[string]float64{
				"fast-b": 100,
			},
		},
	}

	group.applyPendingReservations(states)
	require.Equal(t, 1024.0, states[0].FreeMemoryMB)
	require.Equal(t, 2.0, states[0].FreeCPUCores)
	require.Equal(t, 80.0, states[0].StorageFreeGB["fast-a"])

	candidates, skipped := group.buildCandidateNodes(states)
	require.Empty(t, skipped)

	req := scheduler.Requirement{MemoryMB: 3072, CPUCores: 2, DiskGB: 20}
	selected, err := group.cfg.Scheduler.Select(candidates, scheduler.Reserve{}, req)
	require.NoError(t, err)
	require.Equal(t, "node2", selected.Name)
}

func TestIsManagedTemplate(t *testing.T) {
	t.Parallel()

	group := &Group{
		cfg: Config{
			Pool:                "ci",
			TemplateVMIDMin:     510000,
			TemplateVMIDMax:     510999,
			TemplateNamePrefix:  "glf-template",
			ManagedTemplateTags: []string{"managed-by-fleeting-plugin-proxmox", "managed-role-template-stage"},
		},
	}

	require.True(t, group.isManagedTemplate(proxmoxclient.ClusterResource{
		Type:     "qemu",
		Template: 1,
		Pool:     "ci",
		VMID:     510123,
		Name:     "glf-template-pve2-2001",
		Tags:     "managed-by-fleeting-plugin-proxmox;managed-role-template-stage",
	}))

	require.False(t, group.isManagedTemplate(proxmoxclient.ClusterResource{
		Type:     "qemu",
		Template: 1,
		Pool:     "ci",
		VMID:     500123,
		Name:     "glf-template-pve2-2001",
		Tags:     "managed-by-fleeting-plugin-proxmox;managed-role-template-stage",
	}))

	require.False(t, group.isManagedTemplate(proxmoxclient.ClusterResource{
		Type:     "qemu",
		Template: 1,
		Pool:     "ci",
		VMID:     510123,
		Name:     "glf-template-pve2-2001",
		Tags:     "managed-by-fleeting-plugin-proxmox",
	}))
}

func TestDescriptionValueIgnoresComments(t *testing.T) {
	t.Parallel()

	description := "# bump when replacing disk\ntemplate-version=2\nother=value"
	require.Equal(t, "2", descriptionValue(description, sourceTemplateVersionKey))
	require.Equal(t, "", descriptionValue(description, stagedTemplateVersionKey))
}

func TestShouldReuseManagedTemplate(t *testing.T) {
	t.Parallel()

	require.True(t, shouldReuseManagedTemplate("", ""))
	require.True(t, shouldReuseManagedTemplate("", "1"))
	require.True(t, shouldReuseManagedTemplate("2", "2"))
	require.False(t, shouldReuseManagedTemplate("2", "1"))
	require.False(t, shouldReuseManagedTemplate("2", ""))
}
