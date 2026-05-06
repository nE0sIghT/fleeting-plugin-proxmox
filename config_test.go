package proxmox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

func TestDefaultStateFileIsScopedToConfig(t *testing.T) {
	t.Parallel()

	cfg := pluginConfig{
		APIURL:        "https://pve.example:8006/api2/json",
		TokenID:       "user@pve!token",
		TokenSecret:   "secret",
		Pool:          "gitlab-runners1",
		TemplateVMIDs: []int{2000},
		NamePrefix:    "runner1",
		VMIDRange:     "500000-500999",
		Nodes:         []string{"pve1"},
		IPPoolNetwork: "10.10.20.0/24",
		IPPoolGateway: "10.10.20.1",
		IPPoolRanges:  []string{"10.10.20.100-10.10.20.199"},
	}

	cfg.applyDefaults(provider.Settings{})
	require.Equal(t, "auto", cfg.TemplateStageMode)
	require.Equal(
		t,
		filepath.Join(defaultStateDir, "default-gitlab-runners1-runner1-state.json"),
		cfg.StateFile,
	)
}

func TestTemplateStageModeRequiredNeedsTemplateVMIDRange(t *testing.T) {
	t.Parallel()

	cfg := pluginConfig{
		APIURL:            "https://pve.example:8006/api2/json",
		TokenID:           "user@pve!token",
		TokenSecret:       "secret",
		Pool:              "gitlab-runners1",
		TemplateVMIDs:     []int{2000},
		TemplateStageMode: "required",
		NamePrefix:        "runner1",
		VMIDRange:         "500000-500999",
		Nodes:             []string{"pve1"},
		IPPoolNetwork:     "10.10.20.0/24",
		IPPoolGateway:     "10.10.20.1",
		IPPoolRanges:      []string{"10.10.20.100-10.10.20.199"},
	}

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "missing required plugin config when template_stage_mode=required: template_vmid_range")
}

func TestTemplateStageModeAutoDoesNotRequireTemplateVMIDRangeAtValidation(t *testing.T) {
	t.Parallel()

	cfg := pluginConfig{
		APIURL:            "https://pve.example:8006/api2/json",
		TokenID:           "user@pve!token",
		TokenSecret:       "secret",
		Pool:              "gitlab-runners1",
		TemplateVMIDs:     []int{2000},
		TemplateStageMode: "auto",
		NamePrefix:        "runner1",
		VMIDRange:         "500000-500999",
		Nodes:             []string{"pve1"},
		IPPoolNetwork:     "10.10.20.0/24",
		IPPoolGateway:     "10.10.20.1",
		IPPoolRanges:      []string{"10.10.20.100-10.10.20.199"},
		StateFile:         filepath.Join(t.TempDir(), "state.json"),
	}

	require.NoError(t, cfg.validate(provider.Settings{}))
}

func TestTemplateStageRangeMustNotOverlapVMRange(t *testing.T) {
	t.Parallel()

	cfg := pluginConfig{
		APIURL:            "https://pve.example:8006/api2/json",
		TokenID:           "user@pve!token",
		TokenSecret:       "secret",
		Pool:              "gitlab-runners1",
		TemplateVMIDs:     []int{2000},
		TemplateStageMode: "required",
		TemplateVMIDRange: "500900-501100",
		NamePrefix:        "runner1",
		VMIDRange:         "500000-500999",
		Nodes:             []string{"pve1"},
		IPPoolNetwork:     "10.10.20.0/24",
		IPPoolGateway:     "10.10.20.1",
		IPPoolRanges:      []string{"10.10.20.100-10.10.20.199"},
	}

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "template_vmid_range must not overlap vmid_range")
}

func TestStaticNetworkCanDisableAgentWhenConfiguredExplicitly(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	require.NoError(t, json.Unmarshal([]byte(`{"agent_required":false}`), &cfg))

	require.NoError(t, cfg.validate(provider.Settings{}))
	require.False(t, cfg.AgentRequired)
}

func TestInstanceGroupUnmarshalTracksExplicitAgentRequired(t *testing.T) {
	t.Parallel()

	var group InstanceGroup
	require.NoError(t, json.Unmarshal([]byte(`{"network_mode":"static","agent_required":false}`), &group))

	require.Equal(t, "static", group.NetworkMode)
	require.False(t, group.AgentRequired)
	require.True(t, group.config().agentRequiredSet)
}

func TestDHCPNetworkRequiresAgent(t *testing.T) {
	t.Parallel()

	cfg := validDHCPConfig(t)
	require.NoError(t, json.Unmarshal([]byte(`{"agent_required":false}`), &cfg))

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "network_mode=dhcp requires agent_required=true")
}

func TestDHCPNetworkDoesNotPrepareStateFileDirectory(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "missing", "state.json")
	cfg := validDHCPConfig(t)
	cfg.StateFile = stateFile

	require.NoError(t, cfg.validate(provider.Settings{}))

	_, err := os.Stat(filepath.Dir(stateFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDurationFieldsMustBePositive(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.TaskPollInterval = "0s"
	cfg.CloneTimeout = "-1s"

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "task_poll_interval must be > 0")
	require.ErrorContains(t, err, "clone_timeout must be > 0")
}

func TestIPReuseCooldownMustNotBeNegative(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.IPPoolReuseCooldown = "-1s"

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "ip_pool_reuse_cooldown must be >= 0")
}

func TestMetricsIntervalDefaultsWhenSocketIsConfigured(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.MetricsSocket = "/run/fleeting-plugin-proxmox/metrics.sock"

	require.NoError(t, cfg.validate(provider.Settings{}))
	require.Equal(t, defaultMetricsInterval.String(), cfg.MetricsInterval)
	require.Equal(t, defaultMetricsInterval, cfg.parsedMetricsInterval)
}

func TestMetricsIntervalMustBePositive(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.MetricsSocket = "/run/fleeting-plugin-proxmox/metrics.sock"
	cfg.MetricsInterval = "0s"

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "metrics_interval must be > 0")
}

func TestReserveValuesMustBeNonNegative(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.NodeReserveMemoryMB = -1
	cfg.NodeReserveCPUCores = -1
	cfg.NodeReserveDiskGB = -1
	cfg.NodeMemoryAllocationLimitPercent = -1
	cfg.NodeCPUAllocationLimitPercent = -1

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "node_reserve_memory_mb must be >= 0")
	require.ErrorContains(t, err, "node_reserve_cpu_cores must be >= 0")
	require.ErrorContains(t, err, "node_reserve_disk_gb must be >= 0")
	require.ErrorContains(t, err, "node_memory_allocation_limit_percent must be >= 0")
	require.ErrorContains(t, err, "node_cpu_allocation_limit_percent must be >= 0")
}

func TestDiskReserveRequiresTargetStorages(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.NodeReserveDiskGB = 1

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "node_reserve_disk requires target_storages")
}

func TestNodePoliciesResolveOverridesAndExplicitZero(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.Nodes = []string{"pve1", "pve2", "pve3"}
	cfg.TargetStorages = []string{"nvme0"}
	cfg.NodeReserveMemoryPercent = 10
	cfg.NodeReserveCPUPercent = 10
	cfg.NodeReserveDiskPercent = 10
	cfg.NodeMemoryAllocationLimitPercent = 100
	cfg.NodeCPUAllocationLimitPercent = 100
	cfg.NodePolicies = []nodePolicyConfig{
		{
			Nodes:                     []string{"pve1", "pve2"},
			ReserveMemoryMB:           int64Ptr(32768),
			ReserveMemoryPercent:      intPtr(0),
			ReserveDiskPercent:        intPtr(0),
			CPUAllocationLimitPercent: intPtr(200),
		},
	}

	require.NoError(t, cfg.validate(provider.Settings{}))

	policies := cfg.resolveNodePolicies()
	require.Equal(t, int64(32768), policies["pve1"].Reserve.MemoryMB)
	require.Zero(t, policies["pve1"].Reserve.MemoryPercent)
	require.Zero(t, policies["pve1"].Reserve.DiskPercent)
	require.Equal(t, 100, policies["pve1"].MemoryAllocationLimitPercent)
	require.Equal(t, 200, policies["pve1"].CPUAllocationLimitPercent)
	require.Equal(t, 10, policies["pve3"].Reserve.MemoryPercent)
	require.Equal(t, 10, policies["pve3"].Reserve.CPUPercent)
	require.Equal(t, 10, policies["pve3"].Reserve.DiskPercent)
	require.Equal(t, 100, policies["pve3"].CPUAllocationLimitPercent)
}

func TestNodePoliciesValidateNodes(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.Nodes = []string{"pve1", "pve2"}
	cfg.NodePolicies = []nodePolicyConfig{
		{Nodes: []string{"pve1", "missing"}},
		{Nodes: []string{"pve1"}},
		{},
	}

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, `node_policies[0].nodes contains unknown node "missing"`)
	require.ErrorContains(t, err, `node "pve1" is listed by more than one node_policies entry`)
	require.ErrorContains(t, err, "node_policies[2].nodes must not be empty")
}

func TestNodePolicyValuesMustBeValid(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.NodePolicies = []nodePolicyConfig{
		{
			Nodes:                        []string{"pve1"},
			ReserveMemoryMB:              int64Ptr(-1),
			ReserveCPUCores:              intPtr(-1),
			ReserveDiskGB:                int64Ptr(-1),
			ReserveMemoryPercent:         intPtr(101),
			ReserveCPUPercent:            intPtr(101),
			ReserveDiskPercent:           intPtr(101),
			MemoryAllocationLimitPercent: intPtr(-1),
			CPUAllocationLimitPercent:    intPtr(-1),
		},
	}

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "node_policies[0].reserve_memory_mb must be >= 0")
	require.ErrorContains(t, err, "node_policies[0].reserve_cpu_cores must be >= 0")
	require.ErrorContains(t, err, "node_policies[0].reserve_disk_gb must be >= 0")
	require.ErrorContains(t, err, "node_policies[0].reserve_memory_percent must be between 0 and 100")
	require.ErrorContains(t, err, "node_policies[0].reserve_cpu_percent must be between 0 and 100")
	require.ErrorContains(t, err, "node_policies[0].reserve_disk_percent must be between 0 and 100")
	require.ErrorContains(t, err, "node_policies[0].memory_allocation_limit_percent must be >= 0")
	require.ErrorContains(t, err, "node_policies[0].cpu_allocation_limit_percent must be >= 0")
}

func TestNodePolicyDiskReserveRequiresTargetStorages(t *testing.T) {
	t.Parallel()

	cfg := validStaticConfig(t)
	cfg.NodePolicies = []nodePolicyConfig{
		{Nodes: []string{"pve1"}, ReserveDiskGB: int64Ptr(10)},
	}

	err := cfg.validate(provider.Settings{})
	require.ErrorContains(t, err, "node_policies reserve_disk requires target_storages")
}

func validStaticConfig(t *testing.T) pluginConfig {
	t.Helper()

	return pluginConfig{
		APIURL:        "https://pve.example:8006/api2/json",
		TokenID:       "user@pve!token",
		TokenSecret:   "secret",
		Pool:          "gitlab-runners1",
		TemplateVMIDs: []int{2000},
		NamePrefix:    "runner1",
		VMIDRange:     "500000-500999",
		Nodes:         []string{"pve1"},
		IPPoolNetwork: "10.10.20.0/24",
		IPPoolGateway: "10.10.20.1",
		IPPoolRanges:  []string{"10.10.20.100-10.10.20.199"},
		StateFile:     filepath.Join(t.TempDir(), "state.json"),
	}
}

func validDHCPConfig(t *testing.T) pluginConfig {
	t.Helper()

	cfg := validStaticConfig(t)
	cfg.NetworkMode = "dhcp"
	cfg.IPPoolNetwork = ""
	cfg.IPPoolGateway = ""
	cfg.IPPoolRanges = nil
	cfg.StateFile = ""
	return cfg
}

func int64Ptr(v int64) *int64 {
	return &v
}

func intPtr(v int) *int {
	return &v
}
