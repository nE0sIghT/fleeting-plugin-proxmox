package proxmox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

type mockVM struct {
	Node     string
	VMID     int
	Name     string
	Pool     string
	Storage  string
	Tags     string
	Status   string
	IPConfig string
	DHCPIP   string
}

type mockProxmox struct {
	mu        sync.Mutex
	template  mockVM
	vms       map[int]mockVM
	taskNode  map[string]string
	failClone bool
}

func newMockProxmox() *mockProxmox {
	return &mockProxmox{
		template: mockVM{
			Node:   "node1",
			VMID:   9000,
			Name:   "runner-template",
			Status: "stopped",
		},
		vms:      map[int]mockVM{},
		taskNode: map[string]string{},
	}
}

func (m *mockProxmox) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		write := func(data any) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": data}))
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		switch {
		case r.URL.Path == "/api2/json/version":
			write(map[string]any{"release": "8.2"})
			return
		case r.URL.Path == "/api2/json/pools/ci":
			write(map[string]any{"poolid": "ci"})
			return
		case r.URL.Path == "/api2/json/cluster/resources":
			var out []map[string]any
			out = append(out, map[string]any{
				"type":     "qemu",
				"node":     m.template.Node,
				"vmid":     m.template.VMID,
				"name":     m.template.Name,
				"template": 1,
				"status":   m.template.Status,
				"maxmem":   int64(2 * 1024 * 1024 * 1024),
				"maxdisk":  int64(10 * 1024 * 1024 * 1024),
				"maxcpu":   2,
			})
			out = append(out,
				map[string]any{
					"type":    "storage",
					"node":    "node1",
					"storage": "local-lvm",
					"disk":    int64(300 * 1024 * 1024 * 1024),
					"maxdisk": int64(500 * 1024 * 1024 * 1024),
					"shared":  0,
				},
				map[string]any{
					"type":    "storage",
					"node":    "node1",
					"storage": "ceph-vm",
					"disk":    int64(100 * 1024 * 1024 * 1024),
					"maxdisk": int64(500 * 1024 * 1024 * 1024),
					"shared":  1,
				},
			)
			for _, vm := range m.vms {
				out = append(out, map[string]any{
					"type":    "qemu",
					"node":    vm.Node,
					"vmid":    vm.VMID,
					"name":    vm.Name,
					"pool":    vm.Pool,
					"tags":    vm.Tags,
					"status":  vm.Status,
					"maxmem":  int64(2 * 1024 * 1024 * 1024),
					"maxdisk": int64(10 * 1024 * 1024 * 1024),
					"maxcpu":  2,
				})
			}
			write(out)
			return
		case r.URL.Path == "/api2/json/nodes/node1/status":
			write(map[string]any{
				"cpu": 0.1,
				"cpuinfo": map[string]any{
					"cpus": 16,
				},
				"memory": map[string]any{
					"used":  int64(4 * 1024 * 1024 * 1024),
					"total": int64(32 * 1024 * 1024 * 1024),
				},
				"rootfs": map[string]any{
					"used":  int64(50 * 1024 * 1024 * 1024),
					"total": int64(500 * 1024 * 1024 * 1024),
				},
			})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/api2/json/nodes/node1/qemu/9000/clone":
			require.NoError(t, r.ParseForm())
			vmid, err := strconv.Atoi(r.PostForm.Get("newid"))
			require.NoError(t, err)
			if m.failClone {
				w.WriteHeader(http.StatusInternalServerError)
				write("clone failed")
				return
			}
			m.vms[vmid] = mockVM{
				Node:    r.PostForm.Get("target"),
				VMID:    vmid,
				Name:    r.PostForm.Get("name"),
				Pool:    r.PostForm.Get("pool"),
				Storage: r.PostForm.Get("storage"),
				Status:  "stopped",
				DHCPIP:  fmt.Sprintf("10.10.30.%d", 100+(vmid%50)),
			}
			m.taskNode["UPID:clone"] = "node1"
			write("UPID:clone")
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/node1/qemu/") && strings.HasSuffix(r.URL.Path, "/config"):
			require.NoError(t, r.ParseForm())
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			vm.Pool = r.PostForm.Get("pool")
			vm.Tags = r.PostForm.Get("tags")
			vm.IPConfig = r.PostForm.Get("ipconfig0")
			m.vms[vmid] = vm
			m.taskNode["UPID:config"] = "node1"
			write("UPID:config")
			return
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/status/start"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			vm.Status = "running"
			m.vms[vmid] = vm
			m.taskNode["UPID:start"] = "node1"
			write("UPID:start")
			return
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/status/shutdown"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			vm.Status = "stopped"
			m.vms[vmid] = vm
			m.taskNode["UPID:shutdown"] = "node1"
			write("UPID:shutdown")
			return
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/node1/qemu/"):
			vmid := extractVMID(r.URL.Path)
			delete(m.vms, vmid)
			m.taskNode["UPID:delete"] = "node1"
			write("UPID:delete")
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/node1/tasks/") && strings.HasSuffix(r.URL.Path, "/status"):
			write(map[string]any{"status": "stopped", "exitstatus": "OK"})
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/node1/qemu/") && strings.HasSuffix(r.URL.Path, "/config"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			write(map[string]any{
				"name":      vm.Name,
				"pool":      vm.Pool,
				"tags":      vm.Tags,
				"ipconfig0": vm.IPConfig,
			})
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/node1/qemu/") && strings.HasSuffix(r.URL.Path, "/status/current"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			write(map[string]any{"status": vm.Status, "qmpstatus": vm.Status})
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/node1/qemu/") && strings.HasSuffix(r.URL.Path, "/agent/network-get-interfaces"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			ip := vm.DHCPIP
			if strings.HasPrefix(vm.IPConfig, "ip=") && vm.IPConfig != "ip=dhcp" {
				ip = strings.Split(strings.TrimPrefix(strings.Split(vm.IPConfig, ",")[0], "ip="), "/")[0]
			}
			write(map[string]any{
				"result": []map[string]any{
					{
						"name": "eth0",
						"ip-addresses": []map[string]any{
							{"ip-address": ip, "ip-address-type": "ipv4"},
						},
					},
				},
			})
			return
		}

		t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
	})
}

func TestProviderLifecycle(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		networkMode string
		wantIP      string
	}{
		{name: "static", networkMode: "static", wantIP: "10.10.20.100"},
		{name: "dhcp", networkMode: "dhcp", wantIP: "10.10.30.100"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockProxmox()
			server := httptest.NewServer(mock.handler(t))
			defer server.Close()

			group := &InstanceGroup{}
			group.APIURL = server.URL
			group.TokenID = "root@pam!runner"
			group.TokenSecret = "secret"
			group.ClusterName = "lab"
			group.Pool = "ci"
			group.TemplateVMID = 9000
			group.NamePrefix = "runner"
			group.VMIDRange = "5000-5005"
			group.Nodes = LaxStringList{"node1"}
			group.CloudInitEnabled = true
			group.NetworkMode = tc.networkMode
			group.StateFile = filepath.Join(t.TempDir(), "state.json")
			group.CIUser = "ubuntu"
			group.TargetStorages = LaxStringList{"local-lvm", "ceph-vm"}
			if tc.networkMode == "static" {
				group.IPPoolNetwork = "10.10.20.0/24"
				group.IPPoolGateway = "10.10.20.1"
				group.IPPoolRanges = LaxStringList{"10.10.20.100-10.10.20.101"}
			}

			info, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
			require.NoError(t, err)
			require.Equal(t, "proxmox/lab/ci/runner", info.ID)

			count, err := group.Increase(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, 1, count)

			var ids []string
			err = group.Update(context.Background(), func(instance string, state provider.State) {
				if state == provider.StateRunning {
					ids = append(ids, instance)
				}
			})
			require.NoError(t, err)
			require.Len(t, ids, 1)

			connectInfo, err := group.ConnectInfo(context.Background(), ids[0])
			require.NoError(t, err)
			require.Equal(t, tc.wantIP, connectInfo.InternalAddr)
			require.Equal(t, "ubuntu", connectInfo.Username)
			require.Equal(t, "ceph-vm", mock.vms[5000].Storage)

			deleted, err := group.Decrease(context.Background(), ids)
			require.NoError(t, err)
			require.Equal(t, ids, deleted)
		})
	}
}

func TestCloneRollbackDoesNotDeleteForeignVM(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.failClone = true
	mock.vms[5000] = mockVM{
		Node:    "node1",
		VMID:    5000,
		Name:    "foreign-vm",
		Pool:    "foreign-pool",
		Status:  "running",
		DHCPIP:  "10.99.0.10",
		Storage: "ceph-vm",
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMID = 9000
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node1"}
	group.CloudInitEnabled = true
	group.NetworkMode = "dhcp"
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.NoError(t, err)

	_, err = group.Increase(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "clone failed")

	foreign, ok := mock.vms[5000]
	require.True(t, ok)
	require.Equal(t, "foreign-vm", foreign.Name)
	require.Equal(t, "foreign-pool", foreign.Pool)
}

func extractVMID(path string) int {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part != "qemu" || i+1 >= len(parts) {
			continue
		}
		vmid, err := strconv.Atoi(parts[i+1])
		if err == nil {
			return vmid
		}
	}
	panic(fmt.Sprintf("vmid not found in path %s", path))
}
