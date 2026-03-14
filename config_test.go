package proxmox

import (
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
