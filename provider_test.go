package proxmox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
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
	BootDisk string
	DiskDef  string
	MemoryMB int64
	CPUCores int
	DiskMB   int64
}

type mockProxmox struct {
	mu                       sync.Mutex
	template                 mockVM
	vms                      map[int]mockVM
	taskNode                 map[string]string
	failClone                bool
	failLinkedCloneOnStorage map[string]bool
	downNodes                map[string]bool
	rootFSUsed               int64
	rootFSTotal              int64
	storages                 []map[string]any
	storageCfgs              map[string]map[string]any
}

func newMockProxmox() *mockProxmox {
	return &mockProxmox{
		template: mockVM{
			Node:     "node1",
			VMID:     9000,
			Name:     "runner-template",
			Status:   "stopped",
			BootDisk: "scsi0",
			DiskDef:  "ceph-vm:vm-9000-disk-0,size=10G",
			MemoryMB: 2048,
			CPUCores: 2,
			DiskMB:   10 * 1024,
		},
		vms:                      map[int]mockVM{},
		taskNode:                 map[string]string{},
		failLinkedCloneOnStorage: map[string]bool{},
		downNodes:                map[string]bool{},
		rootFSUsed:               int64(50 * 1024 * 1024 * 1024),
		rootFSTotal:              int64(500 * 1024 * 1024 * 1024),
		storages: []map[string]any{
			{
				"type":       "storage",
				"node":       "node1",
				"storage":    "local-lvm",
				"plugintype": "lvm",
				"disk":       int64(300 * 1024 * 1024 * 1024),
				"maxdisk":    int64(500 * 1024 * 1024 * 1024),
				"shared":     0,
			},
			{
				"type":       "storage",
				"node":       "node1",
				"storage":    "ceph-vm",
				"plugintype": "rbd",
				"disk":       int64(100 * 1024 * 1024 * 1024),
				"maxdisk":    int64(500 * 1024 * 1024 * 1024),
				"shared":     1,
			},
		},
		storageCfgs: map[string]map[string]any{
			"local-lvm": {
				"storage": "local-lvm",
				"nodes":   []string{"node1"},
			},
			"ceph-vm": {
				"storage": "ceph-vm",
				"nodes":   []string{"node1"},
			},
		},
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
		case strings.HasPrefix(r.URL.Path, "/api2/json/storage/"):
			storage := path.Base(r.URL.Path)
			cfg, ok := m.storageCfgs[storage]
			require.True(t, ok, "unexpected storage config request for %s", storage)
			write(cfg)
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
				"maxmem":   m.template.MemoryMB * 1024 * 1024,
				"maxdisk":  m.template.DiskMB * 1024 * 1024,
				"maxcpu":   m.template.CPUCores,
			})
			out = append(out, m.storages...)
			for _, vm := range m.vms {
				out = append(out, map[string]any{
					"type":    "qemu",
					"node":    vm.Node,
					"vmid":    vm.VMID,
					"name":    vm.Name,
					"pool":    vm.Pool,
					"tags":    vm.Tags,
					"status":  vm.Status,
					"maxmem":  vm.MemoryMB * 1024 * 1024,
					"maxdisk": vm.DiskMB * 1024 * 1024,
					"maxcpu":  vm.CPUCores,
				})
			}
			write(out)
			return
		case r.URL.Path == "/api2/json/nodes/node1/status" || r.URL.Path == "/api2/json/nodes/node2/status":
			node := path.Base(path.Dir(r.URL.Path))
			if m.downNodes[node] {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"data":null,"message":"node unavailable"}`))
				return
			}
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
					"used":  m.rootFSUsed,
					"total": m.rootFSTotal,
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
			if m.failLinkedCloneOnStorage[r.PostForm.Get("storage")] && r.PostForm.Get("full") == "0" {
				w.WriteHeader(http.StatusInternalServerError)
				write("Linked clone feature is not supported")
				return
			}
			m.vms[vmid] = mockVM{
				Node:     r.PostForm.Get("target"),
				VMID:     vmid,
				Name:     r.PostForm.Get("name"),
				Pool:     r.PostForm.Get("pool"),
				Storage:  r.PostForm.Get("storage"),
				Status:   "stopped",
				DHCPIP:   fmt.Sprintf("10.10.30.%d", 100+(vmid%50)),
				BootDisk: m.template.BootDisk,
				DiskDef:  m.template.DiskDef,
				MemoryMB: m.template.MemoryMB,
				CPUCores: m.template.CPUCores,
				DiskMB:   m.template.DiskMB,
			}
			m.taskNode["UPID:clone"] = "node1"
			write("UPID:clone")
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/config"):
			require.NoError(t, r.ParseForm())
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			if value := r.PostForm.Get("pool"); value != "" {
				vm.Pool = value
			}
			vm.Tags = r.PostForm.Get("tags")
			vm.IPConfig = r.PostForm.Get("ipconfig0")
			if value := r.PostForm.Get("memory"); value != "" {
				parsed, parseErr := strconv.ParseInt(value, 10, 64)
				require.NoError(t, parseErr)
				vm.MemoryMB = parsed
			}
			if value := r.PostForm.Get("cores"); value != "" {
				parsed, parseErr := strconv.Atoi(value)
				require.NoError(t, parseErr)
				vm.CPUCores = parsed
			}
			m.vms[vmid] = vm
			m.taskNode["UPID:config"] = "node1"
			write("UPID:config")
			return
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/resize"):
			require.NoError(t, r.ParseForm())
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			require.Equal(t, vm.BootDisk, r.PostForm.Get("disk"))
			sizeValue := strings.TrimSuffix(r.PostForm.Get("size"), "M")
			parsed, parseErr := strconv.ParseInt(sizeValue, 10, 64)
			require.NoError(t, parseErr)
			vm.DiskMB = parsed
			vm.DiskDef = fmt.Sprintf("%s:vm-%d-disk-0,size=%dM", vm.Storage, vmid, vm.DiskMB)
			m.vms[vmid] = vm
			m.taskNode["UPID:resize"] = "node1"
			write("UPID:resize")
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/status/start"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			vm.Status = "running"
			m.vms[vmid] = vm
			m.taskNode["UPID:start"] = "node1"
			write("UPID:start")
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/status/stop"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			vm.Status = "stopped"
			m.vms[vmid] = vm
			m.taskNode["UPID:stop"] = "node1"
			write("UPID:stop")
			return
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/"):
			vmid := extractVMID(r.URL.Path)
			delete(m.vms, vmid)
			m.taskNode["UPID:delete"] = "node1"
			write("UPID:delete")
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/tasks/") && strings.HasSuffix(r.URL.Path, "/status"):
			write(map[string]any{"status": "stopped", "exitstatus": "OK"})
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/config"):
			vmid := extractVMID(r.URL.Path)
			if vmid == m.template.VMID {
				write(map[string]any{
					"name":      m.template.Name,
					"pool":      m.template.Pool,
					"tags":      m.template.Tags,
					"bootdisk":  m.template.BootDisk,
					"scsi0":     m.template.DiskDef,
					"ipconfig0": m.template.IPConfig,
				})
				return
			}
			vm := m.vms[vmid]
			write(map[string]any{
				"name":      vm.Name,
				"pool":      vm.Pool,
				"tags":      vm.Tags,
				"ipconfig0": vm.IPConfig,
				"bootdisk":  vm.BootDisk,
				"scsi0":     vm.DiskDef,
			})
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/status/current"):
			vmid := extractVMID(r.URL.Path)
			vm := m.vms[vmid]
			write(map[string]any{"status": vm.Status, "qmpstatus": vm.Status})
			return
		case strings.HasPrefix(r.URL.Path, "/api2/json/nodes/") && strings.Contains(r.URL.Path, "/qemu/") && strings.HasSuffix(r.URL.Path, "/agent/network-get-interfaces"):
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
			group.TemplateVMIDs = []int{9000}
			group.NamePrefix = "runner"
			group.VMIDRange = "5000-5005"
			group.Nodes = LaxStringList{"node1"}
			group.NetworkMode = tc.networkMode
			group.StateFile = filepath.Join(t.TempDir(), "state.json")
			group.CIUser = "ubuntu"
			group.CloneMode = "full"
			group.TargetStorages = LaxStringList{"local-lvm", "ceph-vm"}
			group.VMMemoryMB = 4096
			group.VMCPUCores = 4
			group.VMDiskMB = 20 * 1024
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
			require.Equal(t, int64(4096), mock.vms[5000].MemoryMB)
			require.Equal(t, 4, mock.vms[5000].CPUCores)
			require.Equal(t, int64(20*1024), mock.vms[5000].DiskMB)

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
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node1"}
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

func TestTargetStoragePlacementIgnoresNodeRootFS(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.rootFSUsed = int64(499 * 1024 * 1024 * 1024)
	mock.rootFSTotal = int64(500 * 1024 * 1024 * 1024)

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node1"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "full"
	group.TargetStorages = LaxStringList{"ceph-vm"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.NoError(t, err)

	count, err := group.Increase(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "ceph-vm", mock.vms[5000].Storage)
}

func TestTargetStorageMustExistOnSelectedNode(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.storages = []map[string]any{
		{
			"type":    "storage",
			"node":    "node2",
			"storage": "ceph-vm",
			"disk":    int64(100 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  1,
		},
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node1"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "full"
	group.TargetStorages = LaxStringList{"ceph-vm"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.NoError(t, err)

	_, err = group.Increase(context.Background(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no eligible nodes satisfy configured headroom")
}

func TestInitFailsWhenNodeHasNoLocalTemplateAndNoSharedTargetStorage(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.storages = append(mock.storages, map[string]any{
		"type":    "storage",
		"node":    "node2",
		"storage": "fast-local",
		"disk":    int64(50 * 1024 * 1024 * 1024),
		"maxdisk": int64(500 * 1024 * 1024 * 1024),
		"shared":  0,
	})
	mock.storageCfgs["fast-local"] = map[string]any{
		"storage": "fast-local",
		"nodes":   []string{"node2"},
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node2"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "full"
	group.TargetStorages = LaxStringList{"fast-local"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "node node2 has no usable template")
}

func TestSharedTemplateFallbackUsesFullCloneOnSharedTemplateStorage(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.storages = []map[string]any{
		{
			"type":    "storage",
			"node":    "node1",
			"storage": "ceph-vm",
			"disk":    int64(100 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  1,
		},
		{
			"type":    "storage",
			"node":    "node2",
			"storage": "ceph-vm",
			"disk":    int64(100 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  1,
		},
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node2"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "full"
	group.TargetStorages = LaxStringList{"ceph-vm"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.NoError(t, err)

	count, err := group.Increase(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "node2", mock.vms[5000].Node)
	require.Equal(t, "ceph-vm", mock.vms[5000].Storage)
}

func TestLinkedCloneModeFailsInitWithoutLocalTemplate(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.storages = []map[string]any{
		{
			"type":    "storage",
			"node":    "node1",
			"storage": "ceph-vm",
			"disk":    int64(100 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  1,
		},
		{
			"type":    "storage",
			"node":    "node2",
			"storage": "ceph-vm",
			"disk":    int64(100 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  1,
		},
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node2"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "linked"
	group.TargetStorages = LaxStringList{"ceph-vm"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "clone_mode=linked requires a local template")
}

func TestAutoCloneModeFallsBackToFullForUnsupportedStoragePluginType(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.template = mockVM{
		Node:     "node1",
		VMID:     9000,
		Name:     "runner-template",
		Status:   "stopped",
		BootDisk: "virtio0",
		DiskDef:  "local-lvm:vm-9000-disk-0,size=10G",
		MemoryMB: 2048,
		CPUCores: 2,
		DiskMB:   10 * 1024,
	}
	mock.failLinkedCloneOnStorage["local-lvm"] = true

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node1"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "auto"
	group.TargetStorages = LaxStringList{"local-lvm"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.NoError(t, err)

	count, err := group.Increase(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "local-lvm", mock.vms[5000].Storage)
}

func TestIncreaseSkipsUnavailableNodeAfterInit(t *testing.T) {
	t.Parallel()

	mock := newMockProxmox()
	mock.storages = []map[string]any{
		{
			"type":    "storage",
			"node":    "node1",
			"storage": "local-lvm",
			"disk":    int64(300 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  0,
		},
		{
			"type":    "storage",
			"node":    "node2",
			"storage": "local-lvm",
			"disk":    int64(300 * 1024 * 1024 * 1024),
			"maxdisk": int64(500 * 1024 * 1024 * 1024),
			"shared":  0,
		},
	}

	server := httptest.NewServer(mock.handler(t))
	defer server.Close()

	group := &InstanceGroup{}
	group.APIURL = server.URL
	group.TokenID = "root@pam!runner"
	group.TokenSecret = "secret"
	group.ClusterName = "lab"
	group.Pool = "ci"
	group.TemplateVMIDs = []int{9000, 9001}
	group.NamePrefix = "runner"
	group.VMIDRange = "5000-5005"
	group.Nodes = LaxStringList{"node1", "node2"}
	group.NetworkMode = "dhcp"
	group.CloneMode = "full"
	group.TargetStorages = LaxStringList{"local-lvm"}
	group.StateFile = filepath.Join(t.TempDir(), "state.json")

	mock.template = mockVM{
		Node:     "node1",
		VMID:     9000,
		Name:     "runner-template-node1",
		Status:   "stopped",
		BootDisk: "scsi0",
		DiskDef:  "local-lvm:vm-9000-disk-0,size=10G",
		MemoryMB: 2048,
		CPUCores: 2,
		DiskMB:   10 * 1024,
	}

	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api2/json/cluster/resources" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{
						"type":     "qemu",
						"node":     "node1",
						"vmid":     9000,
						"name":     "runner-template-node1",
						"template": 1,
						"status":   "stopped",
						"maxmem":   int64(2048 * 1024 * 1024),
						"maxdisk":  int64(10 * 1024 * 1024 * 1024),
						"maxcpu":   2,
					},
					{
						"type":     "qemu",
						"node":     "node2",
						"vmid":     9001,
						"name":     "runner-template-node2",
						"template": 1,
						"status":   "stopped",
						"maxmem":   int64(2048 * 1024 * 1024),
						"maxdisk":  int64(10 * 1024 * 1024 * 1024),
						"maxcpu":   2,
					},
					{
						"type":    "storage",
						"node":    "node1",
						"storage": "local-lvm",
						"disk":    int64(300 * 1024 * 1024 * 1024),
						"maxdisk": int64(500 * 1024 * 1024 * 1024),
						"shared":  0,
					},
					{
						"type":    "storage",
						"node":    "node2",
						"storage": "local-lvm",
						"disk":    int64(300 * 1024 * 1024 * 1024),
						"maxdisk": int64(500 * 1024 * 1024 * 1024),
						"shared":  0,
					},
				},
			})
			return
		}
		if r.URL.Path == "/api2/json/nodes/node2/qemu/9001/config" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"name":      "runner-template-node2",
					"bootdisk":  "scsi0",
					"scsi0":     "local-lvm:vm-9001-disk-0,size=10G",
					"ipconfig0": "",
				},
			})
			return
		}
		mock.handler(t).ServeHTTP(w, r)
	})

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{})
	require.NoError(t, err)

	mock.mu.Lock()
	mock.downNodes["node2"] = true
	mock.mu.Unlock()

	count, err := group.Increase(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "node1", mock.vms[5000].Node)
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
