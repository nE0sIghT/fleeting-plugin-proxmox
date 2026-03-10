package instancegroup

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/ippool"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/limiter"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/scheduler"
)

type Config struct {
	ClusterName           string
	Pool                  string
	TemplateVMID          int
	VMIDMin               int
	VMIDMax               int
	NamePrefix            string
	Nodes                 []string
	CloneMode             string
	TargetStorages        []string
	CloneSnapshot         string
	MandatoryTags         []string
	DescriptionTemplate   string
	CloudInitInterface    string
	NetworkMode           string
	CIUser                string
	NameServers           []string
	SearchDomain          string
	TaskPollInterval      time.Duration
	CloneTimeout          time.Duration
	StartTimeout          time.Duration
	ShutdownTimeout       time.Duration
	AgentTimeout          time.Duration
	AgentRequired         bool
	GeneratedSSHPublicKey string
	StaticSSHPublicKeys   []string
	Scheduler             *scheduler.Scheduler
	Reserve               scheduler.Reserve
}

type ManagedInstance struct {
	ID    string
	Node  string
	VMID  int
	Name  string
	State provider.State
	IP    netip.Addr
}

type Group struct {
	client        *proxmoxclient.Client
	log           hclog.Logger
	cfg           Config
	pool          *ippool.Pool
	cloneLimiter  *limiter.Limiter
	startLimiter  *limiter.Limiter
	deleteLimiter *limiter.Limiter

	mu sync.Mutex
	// transient overrides smooth out state transitions while long-running Proxmox tasks complete.
	transient map[string]provider.State
	template  proxmoxclient.ClusterResource
}

func New(client *proxmoxclient.Client, log hclog.Logger, cfg Config, pool *ippool.Pool, cloneLimiter, startLimiter, deleteLimiter *limiter.Limiter) *Group {
	return &Group{
		client:        client,
		log:           log,
		cfg:           cfg,
		pool:          pool,
		cloneLimiter:  cloneLimiter,
		startLimiter:  startLimiter,
		deleteLimiter: deleteLimiter,
		transient:     map[string]provider.State{},
	}
}

func (g *Group) Init(ctx context.Context) error {
	if _, err := g.client.GetPool(ctx, g.cfg.Pool); err != nil {
		return fmt.Errorf("verify pool: %w", err)
	}
	if _, err := g.client.GetVersion(ctx); err != nil {
		return fmt.Errorf("verify api connectivity: %w", err)
	}

	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return err
	}

	templateResource, err := g.findTemplate(resources)
	if err != nil {
		return err
	}
	g.template = templateResource

	for _, node := range g.cfg.Nodes {
		if _, err := g.client.GetNodeStatus(ctx, node); err != nil {
			return fmt.Errorf("verify node %s: %w", node, err)
		}
	}

	managed, err := g.List(ctx)
	if err != nil {
		return err
	}

	active := make(map[string]netip.Addr, len(managed))
	for _, instance := range managed {
		if instance.IP.IsValid() {
			active[instance.ID] = instance.IP
		}
	}

	if g.pool == nil {
		return nil
	}

	return g.pool.Reconcile(ctx, active)
}

func (g *Group) List(ctx context.Context) ([]ManagedInstance, error) {
	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return nil, err
	}

	instances := make([]ManagedInstance, 0)
	for _, resource := range resources {
		if !g.isManaged(resource) {
			continue
		}

		state := mapState(resource.Status)
		id := instanceID(resource.Node, resource.VMID)

		g.mu.Lock()
		if transient, ok := g.transient[id]; ok {
			state = transient
		}
		g.mu.Unlock()

		instance := ManagedInstance{
			ID:    id,
			Node:  resource.Node,
			VMID:  resource.VMID,
			Name:  resource.Name,
			State: state,
		}

		config, err := g.client.GetVMConfig(ctx, resource.Node, resource.VMID)
		if err == nil && g.cfg.NetworkMode == "static" {
			if ip, ok := parseStaticIPv4(config.IPConfig0); ok {
				instance.IP = ip
			}
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

func (g *Group) Increase(ctx context.Context, delta int) ([]string, error) {
	var created []string
	var errs []error

	for i := 0; i < delta; i++ {
		var id string
		err := g.cloneLimiter.Do(ctx, func(ctx context.Context) error {
			var err error
			id, err = g.createOne(ctx)
			return err
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		created = append(created, id)
	}

	return created, errors.Join(errs...)
}

func (g *Group) Decrease(ctx context.Context, ids []string) ([]string, error) {
	var deleted []string
	var errs []error

	for _, id := range ids {
		targetID := id
		err := g.deleteLimiter.Do(ctx, func(ctx context.Context) error {
			return g.deleteOne(ctx, targetID)
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		deleted = append(deleted, id)
	}

	return deleted, errors.Join(errs...)
}

func (g *Group) ConnectInfo(ctx context.Context, id string, settings provider.Settings) (provider.ConnectInfo, error) {
	instance, err := g.Get(ctx, id)
	if err != nil {
		return provider.ConnectInfo{}, err
	}

	if g.cfg.AgentRequired {
		ip, err := g.discoverIPAddress(ctx, instance.Node, instance.VMID, instance.IP, g.cfg.AgentTimeout)
		if err != nil {
			return provider.ConnectInfo{}, err
		}
		instance.IP = ip
	}

	info := provider.ConnectInfo{ConnectorConfig: settings.ConnectorConfig}
	info.ID = id
	info.OS = "linux"
	if info.Arch == "" {
		info.Arch = "amd64"
	}
	if info.Protocol == "" {
		info.Protocol = provider.ProtocolSSH
	}
	if info.ProtocolPort == 0 {
		info.ProtocolPort = provider.DefaultProtocolPorts[provider.ProtocolSSH]
	}
	if info.Username == "" {
		if g.cfg.CIUser != "" {
			info.Username = g.cfg.CIUser
		} else {
			info.Username = "root"
		}
	}
	info.InternalAddr = instance.IP.String()
	info.ExternalAddr = instance.IP.String()
	return info, nil
}

func (g *Group) Heartbeat(ctx context.Context, id string) error {
	instance, err := g.Get(ctx, id)
	if err != nil {
		return provider.ErrInstanceUnhealthy
	}
	if instance.State != provider.StateRunning {
		return provider.ErrInstanceUnhealthy
	}
	if !g.cfg.AgentRequired {
		return nil
	}
	if _, err := g.discoverIPAddress(ctx, instance.Node, instance.VMID, instance.IP, g.cfg.AgentTimeout); err != nil {
		return provider.ErrInstanceUnhealthy
	}
	return nil
}

func (g *Group) Get(ctx context.Context, id string) (ManagedInstance, error) {
	node, vmid, err := parseInstanceID(id)
	if err != nil {
		return ManagedInstance{}, err
	}

	config, err := g.client.GetVMConfig(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return ManagedInstance{}, fmt.Errorf("managed instance %s not found", id)
		}
		return ManagedInstance{}, err
	}
	if !g.isManagedConfig(node, vmid, config) {
		return ManagedInstance{}, fmt.Errorf("managed instance %s not found", id)
	}

	status, err := g.client.GetVMStatus(ctx, node, vmid)
	if err != nil {
		return ManagedInstance{}, err
	}

	instance := ManagedInstance{
		ID:    id,
		Node:  node,
		VMID:  vmid,
		Name:  config.Name,
		State: mapState(status.Status),
	}
	if g.cfg.NetworkMode == "static" {
		if ip, ok := parseStaticIPv4(config.IPConfig0); ok {
			instance.IP = ip
		}
	}

	return instance, nil
}

func (g *Group) createOne(ctx context.Context) (string, error) {
	vmResources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return "", err
	}

	vmid, err := g.allocateVMID(vmResources)
	if err != nil {
		return "", err
	}

	nodeInfos, err := g.collectNodeInfos(ctx)
	if err != nil {
		return "", err
	}

	req := scheduler.Requirement{
		MemoryMB: float64(g.template.MaxMem) / 1024.0 / 1024.0,
		DiskGB:   float64(g.template.MaxDisk) / 1024.0 / 1024.0 / 1024.0,
		CPUCores: g.template.MaxCPU,
	}
	targetNode, err := g.cfg.Scheduler.Select(nodeInfos, g.cfg.Reserve, req)
	if err != nil {
		return "", err
	}

	var storageResources []proxmoxclient.ClusterResource
	if len(g.cfg.TargetStorages) > 0 {
		storageResources, err = g.client.ListClusterResources(ctx, "storage")
		if err != nil {
			return "", err
		}
	}

	targetStorage, err := g.selectStorage(storageResources, targetNode.Name)
	if err != nil {
		return "", err
	}

	id := instanceID(targetNode.Name, vmid)
	var lease ippool.Lease
	if g.cfg.NetworkMode == "static" {
		if g.pool == nil {
			return "", fmt.Errorf("static network mode requires ip pool")
		}
		lease, err = g.pool.Acquire(ctx, id)
		if err != nil {
			return "", err
		}
	}

	g.setTransient(id, provider.StateCreating)
	defer func() {
		g.clearTransient(id)
	}()

	rollback := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), g.cfg.CloneTimeout)
		defer cancel()
		_ = g.safeDestroyProvisioningVM(cleanupCtx, targetNode.Name, vmid, fmt.Sprintf("%s-%d", g.cfg.NamePrefix, vmid))
		if g.pool != nil {
			_ = g.pool.Release(cleanupCtx, id)
		}
	}

	cloneCtx, cancel := context.WithTimeout(ctx, g.cfg.CloneTimeout)
	defer cancel()

	upid, err := g.client.CloneVM(cloneCtx, g.template.Node, g.cfg.TemplateVMID, proxmoxclient.CloneRequest{
		NewID:         vmid,
		Name:          fmt.Sprintf("%s-%d", g.cfg.NamePrefix, vmid),
		TargetNode:    targetNode.Name,
		Pool:          g.cfg.Pool,
		TargetStorage: targetStorage,
		Full:          g.cfg.CloneMode == "full",
		Snapshot:      g.cfg.CloneSnapshot,
	})
	if err != nil {
		rollback()
		return "", err
	}
	if err := g.client.WaitForTask(cloneCtx, g.template.Node, upid, g.cfg.TaskPollInterval); err != nil {
		rollback()
		return "", err
	}

	description, err := g.renderDescription(targetNode.Name, vmid, lease.IP)
	if err != nil {
		rollback()
		return "", err
	}

	sshKeys := append([]string{}, g.cfg.StaticSSHPublicKeys...)
	if g.cfg.GeneratedSSHPublicKey != "" {
		sshKeys = append(sshKeys, g.cfg.GeneratedSSHPublicKey)
	}

	ipConfig := "ip=dhcp"
	if g.cfg.NetworkMode == "static" {
		ipConfig = fmt.Sprintf("ip=%s/%d,gw=%s", lease.IP, lease.Prefix.Bits(), lease.Gateway)
	}

	upid, err = g.client.SetVMConfig(cloneCtx, targetNode.Name, vmid, proxmoxclient.SetConfigRequest{
		CloudInitInterface: g.cfg.CloudInitInterface,
		Pool:               g.cfg.Pool,
		Tags:               g.cfg.MandatoryTags,
		Description:        description,
		IPConfig:           ipConfig,
		CIUser:             g.cfg.CIUser,
		SSHKeys:            sshKeys,
		NameServer:         strings.Join(g.cfg.NameServers, " "),
		SearchDomain:       g.cfg.SearchDomain,
		AgentEnabled:       g.cfg.AgentRequired,
	})
	if err != nil {
		rollback()
		return "", err
	}
	if err := g.client.WaitForTask(cloneCtx, targetNode.Name, upid, g.cfg.TaskPollInterval); err != nil {
		rollback()
		return "", err
	}

	err = g.startLimiter.Do(ctx, func(ctx context.Context) error {
		startCtx, cancel := context.WithTimeout(ctx, g.cfg.StartTimeout)
		defer cancel()

		upid, err := g.client.StartVM(startCtx, targetNode.Name, vmid)
		if err != nil {
			return err
		}
		if err := g.client.WaitForTask(startCtx, targetNode.Name, upid, g.cfg.TaskPollInterval); err != nil {
			return err
		}
		if g.cfg.AgentRequired {
			_, err := g.discoverIPAddress(startCtx, targetNode.Name, vmid, lease.IP, g.cfg.AgentTimeout)
			return err
		}
		return nil
	})
	if err != nil {
		rollback()
		return "", err
	}

	return id, nil
}

func (g *Group) deleteOne(ctx context.Context, id string) error {
	instance, err := g.Get(ctx, id)
	if err != nil {
		return err
	}

	g.setTransient(id, provider.StateDeleting)
	defer g.clearTransient(id)

	status, err := g.client.GetVMStatus(ctx, instance.Node, instance.VMID)
	if err == nil && status.Status == "running" {
		shutdownCtx, cancel := context.WithTimeout(ctx, g.cfg.ShutdownTimeout)
		upid, shutdownErr := g.client.ShutdownVM(shutdownCtx, instance.Node, instance.VMID, g.cfg.ShutdownTimeout)
		if shutdownErr == nil {
			shutdownErr = g.client.WaitForTask(shutdownCtx, instance.Node, upid, g.cfg.TaskPollInterval)
		}
		cancel()
		if shutdownErr != nil {
			stopCtx, stopCancel := context.WithTimeout(ctx, g.cfg.ShutdownTimeout)
			defer stopCancel()
			upid, stopErr := g.client.StopVM(stopCtx, instance.Node, instance.VMID)
			if stopErr != nil {
				return stopErr
			}
			if err := g.client.WaitForTask(stopCtx, instance.Node, upid, g.cfg.TaskPollInterval); err != nil {
				return err
			}
		}
	}

	if err := g.safeDestroyManagedVM(ctx, instance); err != nil {
		return err
	}

	if g.pool != nil {
		return g.pool.Release(ctx, id)
	}
	return nil
}

func (g *Group) safeDestroyManagedVM(ctx context.Context, instance ManagedInstance) error {
	config, found, err := g.lookupVMConfig(ctx, instance.Node, instance.VMID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if !g.isManagedConfig(instance.Node, instance.VMID, config) {
		return fmt.Errorf("refusing to delete unmanaged VM %s/%d", instance.Node, instance.VMID)
	}

	upid, err := g.client.DeleteVM(ctx, instance.Node, instance.VMID)
	if err != nil {
		return err
	}
	return g.client.WaitForTask(ctx, instance.Node, upid, g.cfg.TaskPollInterval)
}

func (g *Group) safeDestroyProvisioningVM(ctx context.Context, node string, vmid int, expectedName string) error {
	config, found, err := g.lookupVMConfig(ctx, node, vmid)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	// Rollback happens before the instance is fully tagged as managed, so we accept only
	// the narrow identity established by this provisioning attempt.
	if config.Template == 1 || config.Name != expectedName {
		return fmt.Errorf("refusing to delete unexpected VM %s/%d during rollback", node, vmid)
	}
	if config.Pool != "" && config.Pool != g.cfg.Pool {
		return fmt.Errorf("refusing to delete VM %s/%d with unexpected pool %q during rollback", node, vmid, config.Pool)
	}
	if vmid < g.cfg.VMIDMin || vmid > g.cfg.VMIDMax {
		return fmt.Errorf("refusing to delete VM %s/%d outside configured VMID range during rollback", node, vmid)
	}

	upid, err := g.client.DeleteVM(ctx, node, vmid)
	if err != nil {
		return err
	}
	return g.client.WaitForTask(ctx, node, upid, g.cfg.TaskPollInterval)
}

func (g *Group) lookupVMConfig(ctx context.Context, node string, vmid int) (proxmoxclient.VMConfig, bool, error) {
	config, err := g.client.GetVMConfig(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return proxmoxclient.VMConfig{}, false, nil
		}
		return proxmoxclient.VMConfig{}, false, err
	}

	return config, true, nil
}

func (g *Group) discoverIPAddress(ctx context.Context, node string, vmid int, expected netip.Addr, timeout time.Duration) (netip.Addr, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(g.cfg.TaskPollInterval)
	defer ticker.Stop()

	for {
		interfaces, err := g.client.GetAgentInterfaces(waitCtx, node, vmid)
		if err == nil {
			for _, iface := range interfaces {
				for _, ip := range iface.IPAddresses {
					if ip.IPType != "ipv4" {
						continue
					}
					// The guest agent can report loopback or link-local addresses before the
					// real workload address appears. Only return a globally useful IPv4.
					addr, parseErr := netip.ParseAddr(ip.IPAddress)
					if parseErr != nil || !addr.Is4() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
						continue
					}
					if expected.IsValid() {
						if addr == expected {
							return addr, nil
						}
						continue
					}
					return addr, nil
				}
			}
		}

		select {
		case <-waitCtx.Done():
			if err != nil {
				return netip.Addr{}, fmt.Errorf("wait for guest agent ip %s: %w", expected, err)
			}
			if expected.IsValid() {
				return netip.Addr{}, fmt.Errorf("guest agent did not report expected IP %s", expected)
			}
			return netip.Addr{}, fmt.Errorf("guest agent did not report any usable IPv4 address")
		case <-ticker.C:
		}
	}
}

func (g *Group) collectNodeInfos(ctx context.Context) ([]scheduler.Node, error) {
	nodes := make([]scheduler.Node, 0, len(g.cfg.Nodes))
	for _, node := range g.cfg.Nodes {
		status, err := g.client.GetNodeStatus(ctx, node)
		if err != nil {
			return nil, err
		}

		totalCPU := float64(status.CPUInfo.CPUs)
		nodes = append(nodes, scheduler.Node{
			Name:         node,
			FreeMemoryMB: float64(status.Memory.Total-status.Memory.Used) / 1024.0 / 1024.0,
			FreeDiskGB:   float64(status.RootFS.Total-status.RootFS.Used) / 1024.0 / 1024.0 / 1024.0,
			FreeCPUCores: totalCPU - (status.CPU * totalCPU),
		})
	}
	return nodes, nil
}

func (g *Group) selectStorage(resources []proxmoxclient.ClusterResource, node string) (string, error) {
	if len(g.cfg.TargetStorages) == 0 {
		return "", nil
	}

	allowed := map[string]struct{}{}
	for _, name := range g.cfg.TargetStorages {
		allowed[name] = struct{}{}
	}

	bestName := ""
	var bestFree int64 = -1

	for _, resource := range resources {
		if resource.Type != "storage" {
			continue
		}
		if _, ok := allowed[resource.Storage]; !ok {
			continue
		}
		if resource.Node != "" && resource.Node != node && resource.Shared != 1 {
			continue
		}
		// Cluster resource storage metrics are point-in-time capacity counters; we use
		// them only to pick the least busy datastore from the caller's allowlist.
		free := resource.MaxDisk - resource.Disk
		if free > bestFree {
			bestFree = free
			bestName = resource.Storage
		}
	}

	if bestName == "" {
		return "", fmt.Errorf("no eligible datastore from target_storages found for node %s", node)
	}

	return bestName, nil
}

func (g *Group) isManagedConfig(node string, vmid int, config proxmoxclient.VMConfig) bool {
	if vmid < g.cfg.VMIDMin || vmid > g.cfg.VMIDMax {
		return false
	}
	if config.Template == 1 {
		return false
	}
	if config.Pool != g.cfg.Pool {
		return false
	}

	tagSet := parseTags(config.Tags)
	for _, tag := range g.cfg.MandatoryTags {
		if _, ok := tagSet[tag]; !ok {
			return false
		}
	}

	return true
}

func parseInstanceID(value string) (string, int, error) {
	node, vmidValue, ok := strings.Cut(value, "/")
	if !ok || node == "" || vmidValue == "" {
		return "", 0, fmt.Errorf("invalid instance ID %q", value)
	}

	vmid, err := strconv.Atoi(vmidValue)
	if err != nil {
		return "", 0, fmt.Errorf("invalid instance ID %q", value)
	}

	return node, vmid, nil
}

func (g *Group) findTemplate(resources []proxmoxclient.ClusterResource) (proxmoxclient.ClusterResource, error) {
	for _, resource := range resources {
		if resource.Type != "qemu" {
			continue
		}
		if resource.VMID != g.cfg.TemplateVMID {
			continue
		}
		if resource.Template != 1 {
			return proxmoxclient.ClusterResource{}, fmt.Errorf("template_vmid %d is not a template", g.cfg.TemplateVMID)
		}
		return resource, nil
	}
	return proxmoxclient.ClusterResource{}, fmt.Errorf("template_vmid %d not found", g.cfg.TemplateVMID)
}

func (g *Group) allocateVMID(resources []proxmoxclient.ClusterResource) (int, error) {
	used := map[int]struct{}{}
	for _, resource := range resources {
		if resource.VMID > 0 {
			used[resource.VMID] = struct{}{}
		}
	}

	for vmid := g.cfg.VMIDMin; vmid <= g.cfg.VMIDMax; vmid++ {
		if _, exists := used[vmid]; exists {
			continue
		}
		return vmid, nil
	}
	return 0, fmt.Errorf("no free VMID in configured range")
}

func (g *Group) isManaged(resource proxmoxclient.ClusterResource) bool {
	if resource.Type != "qemu" || resource.Template == 1 {
		return false
	}
	if resource.Pool != g.cfg.Pool {
		return false
	}
	if resource.VMID < g.cfg.VMIDMin || resource.VMID > g.cfg.VMIDMax {
		return false
	}

	tagSet := parseTags(resource.Tags)
	for _, tag := range g.cfg.MandatoryTags {
		if _, ok := tagSet[tag]; !ok {
			return false
		}
	}

	return true
}

func (g *Group) renderDescription(node string, vmid int, ip netip.Addr) (string, error) {
	ipValue := "dhcp"
	if ip.IsValid() {
		ipValue = ip.String()
	}

	if g.cfg.DescriptionTemplate == "" {
		return fmt.Sprintf("Managed by %s; vmid=%d; node=%s; ip=%s", "fleeting-plugin-proxmox", vmid, node, ipValue), nil
	}

	tmpl, err := template.New("description").Parse(g.cfg.DescriptionTemplate)
	if err != nil {
		return "", err
	}

	data := map[string]any{
		"Node": node,
		"VMID": vmid,
		"IP":   ipValue,
		"Pool": g.cfg.Pool,
	}

	var builder strings.Builder
	if err := tmpl.Execute(&builder, data); err != nil {
		return "", err
	}
	return builder.String(), nil
}

func (g *Group) setTransient(id string, state provider.State) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.transient[id] = state
}

func (g *Group) clearTransient(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.transient, id)
}

func instanceID(node string, vmid int) string {
	return fmt.Sprintf("%s/%d", node, vmid)
}

func mapState(status string) provider.State {
	switch status {
	case "running":
		return provider.StateRunning
	case "stopped", "paused":
		return provider.StateCreating
	default:
		return provider.StateCreating
	}
}

func parseStaticIPv4(value string) (netip.Addr, bool) {
	parts := strings.Split(value, ",")
	for _, part := range parts {
		if !strings.HasPrefix(part, "ip=") {
			continue
		}
		value := strings.TrimPrefix(part, "ip=")
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return netip.Addr{}, false
		}
		if prefix.Addr().Is4() {
			return prefix.Addr(), true
		}
	}
	return netip.Addr{}, false
}

func parseTags(value string) map[string]struct{} {
	out := map[string]struct{}{}
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ';' || r == ',' || r == ' '
	})
	for _, field := range fields {
		if field == "" {
			continue
		}
		out[field] = struct{}{}
	}
	return out
}
