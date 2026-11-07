package instancegroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
)

func TestCleanupAbsentVMDoesNotRequireConfigAudit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api2/json/cluster/nextid" {
			_, _ = w.Write([]byte(`{"data":"5000"}`))
			return
		}
		t.Error("cleanup must not request config for an absent VM", r.URL.Path)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	client, err := proxmoxclient.New(proxmoxclient.Config{BaseURL: server.URL})
	require.NoError(t, err)
	group := &Group{client: client}
	require.NoError(t, group.safeDestroyProvisioningVM(context.Background(), "node1", 5000, "runner-5000"))
}
