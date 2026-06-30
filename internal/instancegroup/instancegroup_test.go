package instancegroup

import (
	"context"
	"errors"
	"testing"
	"time"

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
	require.Equal(t, 3072.0, states[0].AllocatedMemoryMB)
	require.Equal(t, 2.0, states[0].FreeCPUCores)
	require.Equal(t, 2.0, states[0].AllocatedCPUCores)
	require.Equal(t, 80.0, states[0].StorageFreeGB["fast-a"])

	candidates, skipped := group.buildCandidateNodes(states)
	require.Empty(t, skipped)

	req := scheduler.Requirement{MemoryMB: 3072, CPUCores: 2, DiskGB: 20}
	selected, err := group.cfg.Scheduler.Select(candidates, scheduler.Reserve{}, req)
	require.NoError(t, err)
	require.Equal(t, "node2", selected.Name)
}

func TestClonePlacementQuarantineIsScopedAndExpires(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	key := clonePlacementKey{Node: "node1", Storage: "fast-a", VMID: 5000}
	group := &Group{
		cfg: Config{
			TargetStorages: []string{"fast-a", "fast-b"},
		},
		cloneQuarantine: map[clonePlacementKey]time.Time{
			key: now.Add(cloneCollisionQuarantineTTL),
		},
	}
	states := []nodePlanState{
		{
			Name: "node1",
			StorageFreeGB: map[string]float64{
				"fast-a": 100,
				"fast-b": 80,
			},
		},
	}

	candidates, skipped, quarantineSkipped := group.buildCandidateNodesForVMID(states, 5000, now)
	require.True(t, quarantineSkipped)
	require.Len(t, candidates, 1)
	require.Equal(t, "fast-b", candidates[0].TargetStorage)
	require.NotEmpty(t, skipped)

	candidates, skipped, quarantineSkipped = group.buildCandidateNodesForVMID(states, 5001, now)
	require.False(t, quarantineSkipped)
	require.Len(t, candidates, 1)
	require.Equal(t, "fast-a", candidates[0].TargetStorage)
	require.Empty(t, skipped)

	candidates, skipped, quarantineSkipped = group.buildCandidateNodesForVMID(states, 5000, now.Add(cloneCollisionQuarantineTTL))
	require.False(t, quarantineSkipped)
	require.Len(t, candidates, 1)
	require.Equal(t, "fast-a", candidates[0].TargetStorage)
	require.Empty(t, skipped)
	require.NotContains(t, group.cloneQuarantine, key)
}

func TestCloneVolumeCollisionDetection(t *testing.T) {
	t.Parallel()

	err := errors.New("clone failed: disk image '/mnt/pve/nvme4/images/520002/vm-520002-cloudinit.raw' already exists")
	require.True(t, isCloneVolumeCollision(err, 520002))
	require.False(t, isCloneVolumeCollision(err, 520003))
	require.False(t, isCloneVolumeCollision(errors.New("clone failed: connection reset"), 520002))
}

func TestFormatDurationIsHumanReadable(t *testing.T) {
	t.Parallel()

	require.Equal(t, "45ms", formatDuration(44*time.Millisecond+600*time.Microsecond))
	require.Equal(t, "2.042s", formatDuration(2*time.Second+42*time.Millisecond+134*time.Microsecond))
	require.Equal(t, "3m1.2s", formatDuration(3*time.Minute+1200*time.Millisecond))
}

func TestOperationIDsAreUniqueAndDescribeOperation(t *testing.T) {
	t.Parallel()

	group := &Group{}
	first := group.nextOperationID("inc")
	second := group.nextOperationID("inc")

	require.Regexp(t, `^inc-[0-9a-f]+-[0-9a-f]+$`, first)
	require.NotEqual(t, first, second)
}

func TestApplyAllocatedResourcesCountsRunningVMs(t *testing.T) {
	t.Parallel()

	group := &Group{
		pendingVMIDs: map[int]struct{}{5003: {}},
	}
	states := []nodePlanState{
		{Name: "node1"},
		{Name: "node2"},
	}
	resources := []proxmoxclient.ClusterResource{
		{Type: "qemu", Node: "node1", VMID: 5001, Status: "running", MaxMem: 4 * 1024 * 1024 * 1024, MaxCPU: 2},
		{Type: "qemu", Node: "node1", VMID: 5002, Status: "stopped", MaxMem: 8 * 1024 * 1024 * 1024, MaxCPU: 4},
		{Type: "qemu", Node: "node1", VMID: 5004, Status: "stopped", Tags: "managed-by-fleeting-plugin-proxmox", MaxMem: 32 * 1024 * 1024 * 1024, MaxCPU: 16},
		{Type: "qemu", Node: "node1", VMID: 5003, Status: "running", MaxMem: 16 * 1024 * 1024 * 1024, MaxCPU: 8},
		{Type: "qemu", Node: "node1", VMID: 2001, Template: 1, Status: "running", MaxMem: 8 * 1024 * 1024 * 1024, MaxCPU: 4},
		{Type: "lxc", Node: "node1", VMID: 6001, Status: "running", MaxMem: 8 * 1024 * 1024 * 1024, MaxCPU: 4},
		{Type: "qemu", Node: "node3", VMID: 7001, Status: "running", MaxMem: 8 * 1024 * 1024 * 1024, MaxCPU: 4},
	}

	group.applyAllocatedResources(states, resources)

	require.Equal(t, 36864.0, states[0].AllocatedMemoryMB)
	require.Equal(t, 18.0, states[0].AllocatedCPUCores)
	require.Zero(t, states[1].AllocatedMemoryMB)
	require.Zero(t, states[1].AllocatedCPUCores)
}

func TestShouldCountAllocatedResource(t *testing.T) {
	t.Parallel()

	require.True(t, shouldCountAllocatedResource(proxmoxclient.ClusterResource{
		Type:   "qemu",
		Status: "running",
	}))
	require.True(t, shouldCountAllocatedResource(proxmoxclient.ClusterResource{
		Type:   "qemu",
		Status: "stopped",
		Tags:   "managed-by-fleeting-plugin-proxmox",
	}))
	require.False(t, shouldCountAllocatedResource(proxmoxclient.ClusterResource{
		Type:   "qemu",
		Status: "stopped",
	}))
	require.False(t, shouldCountAllocatedResource(proxmoxclient.ClusterResource{
		Type:     "qemu",
		Template: 1,
		Status:   "stopped",
		Tags:     "managed-by-fleeting-plugin-proxmox",
	}))
	require.False(t, shouldCountAllocatedResource(proxmoxclient.ClusterResource{
		Type:   "lxc",
		Status: "running",
	}))
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

func TestResolveDiskDeviceSkipsCloudInitCDROM(t *testing.T) {
	t.Parallel()

	group := &Group{}
	config := proxmoxclient.VMConfig{
		BootDisk: "scsi0",
		SCSI0:    "nvme2:vm-21001-cloudinit,media=cdrom",
		DiskDevices: map[string]string{
			"scsi0": "nvme2:vm-21001-cloudinit,media=cdrom",
			"scsi1": "nvme2:vm-21001-disk-0,size=64G",
		},
	}

	disk, err := group.resolveDiskDevice(config)
	require.NoError(t, err)
	require.Equal(t, "scsi1", disk)
}

func TestResolveDiskDeviceRejectsExplicitCDROM(t *testing.T) {
	t.Parallel()

	group := &Group{cfg: Config{VMDiskDevice: "scsi0"}}
	_, err := group.resolveDiskDevice(proxmoxclient.VMConfig{
		SCSI0: "nvme2:vm-21001-cloudinit,media=cdrom",
	})
	require.ErrorContains(t, err, `vm_disk_device "scsi0" is not a resizable disk device`)
}

func TestIncreaseIgnoresNonPositiveDelta(t *testing.T) {
	t.Parallel()

	group := &Group{}

	ids, err := group.Increase(context.Background(), 0)
	require.NoError(t, err)
	require.Empty(t, ids)

	ids, err = group.Increase(context.Background(), -1)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestAllocateManagedTemplateVMIDSkipsUsedVMIDs(t *testing.T) {
	t.Parallel()

	group := &Group{cfg: Config{TemplateVMIDMin: 510000, TemplateVMIDMax: 510002}}

	vmid, err := group.allocateManagedTemplateVMID(map[int]struct{}{
		510000: {},
		510001: {},
	})
	require.NoError(t, err)
	require.Equal(t, 510002, vmid)
}
