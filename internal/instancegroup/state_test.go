package instancegroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
)

func TestMapState(t *testing.T) {
	tests := []struct {
		status, qmpStatus string
		want              provider.State
	}{
		{"running", "running", provider.StateRunning},
		{"running", "", provider.StateRunning},
		{"running", "paused", provider.StateSuspended},
		{"running", "suspended", provider.StateSuspended},
		{"stopped", "stopped", provider.StateCreating},
		{"stopped", "", provider.StateCreating},
		{"", "", provider.StateCreating},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, mapState(tt.status, tt.qmpStatus), "status=%q qmpstatus=%q", tt.status, tt.qmpStatus)
	}
}

func TestListReportsPausedVMAsSuspended(t *testing.T) {
	var mu sync.Mutex
	statusCalls := map[string]int{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api2/json/cluster/resources":
			// /cluster/resources has no qmpstatus: a paused VM is reported as running.
			_, _ = w.Write([]byte(`{"data":[
				{"type":"qemu","node":"node1","vmid":1001,"name":"runner-1001","pool":"gitlab-runners","status":"running"},
				{"type":"qemu","node":"node1","vmid":1002,"name":"runner-1002","pool":"gitlab-runners","status":"running"},
				{"type":"qemu","node":"node1","vmid":1003,"name":"runner-1003","pool":"gitlab-runners","status":"stopped"}
			]}`))
		case "/api2/json/nodes/node1/qemu/1001/status/current":
			mu.Lock()
			statusCalls["1001"]++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"data":{"status":"running","qmpstatus":"paused"}}`))
		case "/api2/json/nodes/node1/qemu/1002/status/current":
			mu.Lock()
			statusCalls["1002"]++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"data":{"status":"running","qmpstatus":"running"}}`))
		case "/api2/json/nodes/node1/qemu/1003/status/current":
			mu.Lock()
			statusCalls["1003"]++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"data":{"status":"stopped","qmpstatus":"stopped"}}`))
		case "/api2/json/nodes/node1/qemu/1001/config",
			"/api2/json/nodes/node1/qemu/1002/config",
			"/api2/json/nodes/node1/qemu/1003/config":
			_, _ = w.Write([]byte(`{"data":{}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)

	client, err := proxmoxclient.New(proxmoxclient.Config{BaseURL: server.URL})
	require.NoError(t, err)
	group := &Group{
		client: client,
		cfg:    Config{Pool: "gitlab-runners", VMIDMin: 1000, VMIDMax: 2000, NamePrefix: "runner"},
	}

	instances, err := group.List(context.Background())
	require.NoError(t, err)

	states := map[string]provider.State{}
	for _, instance := range instances {
		states[instance.ID] = instance.State
	}
	require.Equal(t, map[string]provider.State{
		instanceID("node1", 1001): provider.StateSuspended,
		instanceID("node1", 1002): provider.StateRunning,
		instanceID("node1", 1003): provider.StateCreating,
	}, states)
	require.Zero(t, statusCalls["1003"], "stopped VMs cannot be paused, so their status should not be queried")
}
