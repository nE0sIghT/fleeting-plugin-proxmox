package proxmox

import (
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

	err := cfg.validate(provider.Settings{})
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(os.TempDir(), Version.Name, "default-gitlab-runners1-runner1-state.json"),
		cfg.StateFile,
	)
}
