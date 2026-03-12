package instancegroup

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
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
	TemplateVMIDs         []int
	VMIDMin               int
	VMIDMax               int
	NamePrefix            string
	Nodes                 []string
	CloneMode             string
	TargetStorages        []string
	CloneSnapshot         string
	VMMemoryMB            int64
	VMCPUCores            int
	VMDiskMB              int64
	VMDiskDevice          string
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
	transient       map[string]provider.State
	pendingByNode   map[string]pendingReservation
	pendingVMIDs    map[int]struct{}
	templatesByNode map[string]templateChoice
	storageNodes    map[string]map[string]struct{}
	templateSizing  proxmoxclient.ClusterResource
	templateDisk    string
}

var diskSizePattern = regexp.MustCompile(`(?:^|,)size=([0-9]+(?:\.[0-9]+)?)([KMGT])(?:,|$)`)

type provisionPlan struct {
	TemplateNode    string
	TemplateVMID    int
	TemplateStorage string
	Node            string
	TargetStorage   string
	VMID            int
	Requirement     scheduler.Requirement
}

type pendingReservation struct {
	MemoryMB  float64
	CPUCores  float64
	StorageGB map[string]float64
}

type nodePlanState struct {
	Name            string
	TemplateNode    string
	TemplateVMID    int
	TemplateStorage string
	TotalMemoryMB   float64
	FreeMemoryMB    float64
	TotalCPUCores   float64
	FreeCPUCores    float64
	StorageTotalGB  map[string]float64
	StorageFreeGB   map[string]float64
}

type templateChoice struct {
	Resource     proxmoxclient.ClusterResource
	Storage      string
	StorageNodes map[string]struct{}
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
		pendingByNode: map[string]pendingReservation{},
		pendingVMIDs:  map[int]struct{}{},
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

	templateResources, err := g.findTemplates(resources)
	if err != nil {
		return err
	}

	storageResources, err := g.client.ListClusterResources(ctx, "storage")
	if err != nil {
		return err
	}

	for _, node := range g.cfg.Nodes {
		if _, err := g.client.GetNodeStatus(ctx, node); err != nil {
			return fmt.Errorf("verify node %s: %w", node, err)
		}
	}

	templateStorageNodes, err := g.loadTemplateStorageNodes(ctx, templateResources, storageResources)
	if err != nil {
		return err
	}

	templatesByNode, templateSizing, templateDisk, err := g.resolveNodeTemplates(ctx, templateResources, templateStorageNodes)
	if err != nil {
		return err
	}
	g.storageNodes = indexStorageNodes(storageResources)
	g.templatesByNode = templatesByNode
	g.templateSizing = templateSizing
	g.templateDisk = templateDisk
	g.logEffectiveTopology()

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
	plans, planErr := g.planIncrease(ctx, delta)
	if len(plans) == 0 {
		return nil, planErr
	}

	type result struct {
		id  string
		err error
	}

	workerCount := g.provisionWorkerCount()
	jobs := make(chan provisionPlan)
	results := make(chan result, len(plans))
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for plan := range jobs {
				id, err := g.provisionOne(ctx, plan)
				g.releasePendingPlan(plan)
				results <- result{id: id, err: err}
			}
		}()
	}

	for _, plan := range plans {
		jobs <- plan
	}
	close(jobs)
	wg.Wait()
	close(results)

	created := make([]string, 0, len(plans))
	var errs []error
	if planErr != nil {
		errs = append(errs, planErr)
	}
	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
			continue
		}
		created = append(created, res.id)
	}

	if len(created) > 0 {
		if err := errors.Join(errs...); err != nil {
			g.log.Warn("provisioning completed with partial failures", "created", len(created), "requested", len(plans), "error", err)
		}
		return created, nil
	}

	return created, errors.Join(errs...)
}

func (g *Group) Decrease(ctx context.Context, ids []string) ([]string, error) {
	type result struct {
		id  string
		err error
	}

	workerCount := g.deleteLimiter.Capacity()
	if workerCount <= 0 {
		workerCount = 1
	}

	jobs := make(chan string)
	results := make(chan result, len(ids))
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				err := g.deleteLimiter.Do(ctx, func(ctx context.Context) error {
					return g.deleteOne(ctx, id)
				})
				results <- result{id: id, err: err}
			}
		}()
	}

	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	wg.Wait()
	close(results)

	var deleted []string
	var errs []error
	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
			continue
		}
		deleted = append(deleted, res.id)
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

	resource, found, err := g.lookupManagedResource(ctx, node, vmid)
	if err != nil {
		return ManagedInstance{}, err
	}
	if !found {
		return ManagedInstance{}, fmt.Errorf("managed instance %s not found", id)
	}

	config, err := g.client.GetVMConfig(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return ManagedInstance{}, fmt.Errorf("managed instance %s not found", id)
		}
		return ManagedInstance{}, err
	}

	status, err := g.client.GetVMStatus(ctx, node, vmid)
	if err != nil {
		return ManagedInstance{}, err
	}

	instance := ManagedInstance{
		ID:    id,
		Node:  node,
		VMID:  vmid,
		Name:  resource.Name,
		State: mapState(status.Status),
	}
	if g.cfg.NetworkMode == "static" {
		if ip, ok := parseStaticIPv4(config.IPConfig0); ok {
			instance.IP = ip
		}
	}

	return instance, nil
}

func (g *Group) planIncrease(ctx context.Context, delta int) ([]provisionPlan, error) {
	vmResources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return nil, err
	}

	var storageResources []proxmoxclient.ClusterResource
	storageNodes := map[string]map[string]struct{}{}
	if len(g.cfg.TargetStorages) > 0 {
		storageResources, err = g.client.ListClusterResources(ctx, "storage")
		if err != nil {
			return nil, err
		}
		storageNodes = indexStorageNodes(storageResources)
	}

	states, skippedReasons, err := g.collectNodePlanStates(ctx, storageResources, storageNodes)
	if err != nil {
		return nil, err
	}

	req := scheduler.Requirement{
		MemoryMB: g.requiredMemoryMB(),
		DiskGB:   g.requiredDiskGB(),
		CPUCores: g.requiredCPUCores(),
	}
	if len(g.cfg.TargetStorages) == 0 {
		req.DiskGB = 0
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	vmids, vmidErr := g.allocateVMIDs(vmResources, delta)
	if len(vmids) == 0 && vmidErr != nil {
		return nil, vmidErr
	}
	g.applyPendingReservations(states)
	plans := make([]provisionPlan, 0, len(vmids))
	var errs []error
	if vmidErr != nil {
		errs = append(errs, vmidErr)
	}

	for _, vmid := range vmids {
		nodeInfos, dynamicSkipped := g.buildCandidateNodes(states)
		if len(nodeInfos) == 0 {
			reasons := append(append([]string{}, skippedReasons...), dynamicSkipped...)
			if len(reasons) == 0 {
				reasons = scheduler.Diagnose(g.diagnosticNodes(states), g.cfg.Reserve, req)
			}
			placementErr := &scheduler.PlacementError{Reasons: reasons}
			if len(placementErr.Reasons) > 0 {
				g.log.Warn("placement rejected", "reasons", strings.Join(placementErr.Reasons, "; "))
			}
			errs = append(errs, placementErr)
			break
		}

		targetNode, err := g.cfg.Scheduler.Select(nodeInfos, g.cfg.Reserve, req)
		if err != nil {
			var placementErr *scheduler.PlacementError
			if errors.As(err, &placementErr) {
				placementErr.Reasons = append(append([]string{}, skippedReasons...), dynamicSkipped...)
				if len(placementErr.Reasons) == 0 {
					placementErr.Reasons = scheduler.Diagnose(g.diagnosticNodes(states), g.cfg.Reserve, req)
				}
				if len(placementErr.Reasons) > 0 {
					g.log.Warn("placement rejected", "reasons", strings.Join(placementErr.Reasons, "; "))
				}
			}
			errs = append(errs, err)
			break
		}

		plans = append(plans, provisionPlan{
			TemplateNode:    targetNode.TemplateNode,
			TemplateVMID:    targetNode.TemplateVMID,
			TemplateStorage: stateTemplateStorage(states, targetNode.Name),
			Node:            targetNode.Name,
			TargetStorage:   targetNode.TargetStorage,
			VMID:            vmid,
			Requirement:     req,
		})
		g.reservePlannedResources(states, targetNode, req)
	}

	g.registerPendingPlans(plans)

	return plans, errors.Join(errs...)
}

func (g *Group) provisionOne(ctx context.Context, plan provisionPlan) (string, error) {
	id := instanceID(plan.Node, plan.VMID)
	var lease ippool.Lease
	var err error
	log := g.log.With("instance", id, "node", plan.Node, "vmid", plan.VMID, "storage", plan.TargetStorage)
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
		_ = g.safeDestroyProvisioningVM(cleanupCtx, plan.Node, plan.VMID, fmt.Sprintf("%s-%d", g.cfg.NamePrefix, plan.VMID))
		if g.pool != nil {
			// Failed provisioning must not leave leases behind or put them into reuse cooldown.
			_ = g.pool.Forget(cleanupCtx, id)
		}
	}

	cloneCtx, cancel := context.WithTimeout(ctx, g.cfg.CloneTimeout)
	defer cancel()

	log.Info("provisioning started")
	cloneStart := time.Now()
	err = g.cloneLimiter.Do(cloneCtx, func(ctx context.Context) error {
		log.Info("clone started")
		effectiveMode := g.effectiveCloneMode(plan)
		targetStorage := plan.TargetStorage
		fullClone := effectiveMode == "full"
		if effectiveMode == "linked" {
			targetStorage = ""
		}
		upid, err := g.client.CloneVM(ctx, plan.TemplateNode, plan.TemplateVMID, proxmoxclient.CloneRequest{
			NewID:         plan.VMID,
			Name:          fmt.Sprintf("%s-%d", g.cfg.NamePrefix, plan.VMID),
			TargetNode:    plan.Node,
			Pool:          g.cfg.Pool,
			TargetStorage: targetStorage,
			Full:          fullClone,
			Snapshot:      g.cfg.CloneSnapshot,
		})
		if err != nil {
			return err
		}
		return g.client.WaitForTask(ctx, plan.TemplateNode, upid, g.cfg.TaskPollInterval)
	})
	if err != nil {
		log.Error("clone failed", "duration", time.Since(cloneStart), "error", err)
		rollback()
		return "", err
	}
	log.Info("clone completed", "duration", time.Since(cloneStart))

	description, err := g.renderDescription(plan.Node, plan.VMID, lease.IP)
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

	configStart := time.Now()
	log.Info("config started")
	upid, err := g.client.SetVMConfig(cloneCtx, plan.Node, plan.VMID, proxmoxclient.SetConfigRequest{
		CloudInitInterface: g.cfg.CloudInitInterface,
		Tags:               g.cfg.MandatoryTags,
		Description:        description,
		IPConfig:           ipConfig,
		MemoryMB:           g.cfg.VMMemoryMB,
		CPUCores:           g.cfg.VMCPUCores,
		CIUser:             g.cfg.CIUser,
		SSHKeys:            sshKeys,
		NameServer:         strings.Join(g.cfg.NameServers, " "),
		SearchDomain:       g.cfg.SearchDomain,
		AgentEnabled:       g.cfg.AgentRequired,
		DisableCIUpgrade:   true,
	})
	if err != nil {
		log.Error("config failed", "duration", time.Since(configStart), "error", err)
		rollback()
		return "", err
	}
	if err := g.client.WaitForTask(cloneCtx, plan.Node, upid, g.cfg.TaskPollInterval); err != nil {
		log.Error("config wait failed", "duration", time.Since(configStart), "error", err)
		rollback()
		return "", err
	}
	log.Info("config completed", "duration", time.Since(configStart))

	if g.cfg.VMDiskMB > 0 {
		resizeStart := time.Now()
		log.Info("resize started", "disk_device", g.templateDisk, "disk_mb", g.cfg.VMDiskMB)
		upid, err = g.client.ResizeVMDisk(cloneCtx, plan.Node, plan.VMID, g.templateDisk, g.cfg.VMDiskMB)
		if err != nil {
			log.Error("resize failed", "duration", time.Since(resizeStart), "error", err)
			rollback()
			return "", err
		}
		if err := g.client.WaitForTask(cloneCtx, plan.Node, upid, g.cfg.TaskPollInterval); err != nil {
			log.Error("resize wait failed", "duration", time.Since(resizeStart), "error", err)
			rollback()
			return "", err
		}
		log.Info("resize completed", "duration", time.Since(resizeStart))
	}

	startPhase := time.Now()
	err = g.startLimiter.Do(ctx, func(ctx context.Context) error {
		startCtx, cancel := context.WithTimeout(ctx, g.cfg.StartTimeout)
		defer cancel()

		log.Info("start started")
		upid, err := g.client.StartVM(startCtx, plan.Node, plan.VMID)
		if err != nil {
			return err
		}
		if err := g.client.WaitForTask(startCtx, plan.Node, upid, g.cfg.TaskPollInterval); err != nil {
			return err
		}
		if g.cfg.AgentRequired {
			_, err := g.discoverIPAddress(startCtx, plan.Node, plan.VMID, lease.IP, g.cfg.AgentTimeout)
			return err
		}
		return nil
	})
	if err != nil {
		log.Error("start failed", "duration", time.Since(startPhase), "error", err)
		rollback()
		return "", err
	}
	log.Info("start completed", "duration", time.Since(startPhase))
	log.Info("provisioning completed")

	return id, nil
}

func (g *Group) effectiveCloneMode(plan provisionPlan) string {
	switch g.cfg.CloneMode {
	case "linked":
		return "linked"
	case "full":
		return "full"
	default:
		if plan.TemplateNode == plan.Node {
			if plan.TargetStorage == "" || plan.TargetStorage == plan.TemplateStorage {
				return "linked"
			}
		}
		return "full"
	}
}

func (g *Group) deleteOne(ctx context.Context, id string) error {
	instance, err := g.Get(ctx, id)
	if err != nil {
		return err
	}

	g.setTransient(id, provider.StateDeleting)
	defer g.clearTransient(id)

	if err := g.ensureVMStopped(ctx, instance.Node, instance.VMID); err != nil {
		return err
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
	_, found, err := g.lookupManagedResource(ctx, instance.Node, instance.VMID)
	if err != nil {
		return err
	}
	if !found {
		return nil
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
	if err := g.ensureVMStopped(ctx, node, vmid); err != nil {
		return err
	}

	upid, err := g.client.DeleteVM(ctx, node, vmid)
	if err != nil {
		return err
	}
	return g.client.WaitForTask(ctx, node, upid, g.cfg.TaskPollInterval)
}

func (g *Group) ensureVMStopped(ctx context.Context, node string, vmid int) error {
	status, err := g.client.GetVMStatus(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return nil
		}
		return err
	}
	if status.Status != "running" {
		return nil
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, g.cfg.ShutdownTimeout)
	defer stopCancel()
	upid, stopErr := g.client.StopVM(stopCtx, node, vmid)
	if stopErr != nil {
		return stopErr
	}
	return g.client.WaitForTask(stopCtx, node, upid, g.cfg.TaskPollInterval)
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

func (g *Group) lookupManagedResource(ctx context.Context, node string, vmid int) (proxmoxclient.ClusterResource, bool, error) {
	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return proxmoxclient.ClusterResource{}, false, err
	}
	for _, resource := range resources {
		if resource.Node != node || resource.VMID != vmid {
			continue
		}
		if !g.isManaged(resource) {
			return proxmoxclient.ClusterResource{}, false, nil
		}
		return resource, true, nil
	}
	return proxmoxclient.ClusterResource{}, false, nil
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

func (g *Group) collectNodePlanStates(ctx context.Context, storageResources []proxmoxclient.ClusterResource, storageNodes map[string]map[string]struct{}) ([]nodePlanState, []string, error) {
	nodes := make([]nodePlanState, 0, len(g.cfg.Nodes))
	skipped := make([]string, 0)
	for _, node := range g.cfg.Nodes {
		templateChoice, ok := g.templatesByNode[node]
		if !ok {
			skipped = append(skipped, fmt.Sprintf("%s: no usable template", node))
			continue
		}

		status, err := g.client.GetNodeStatus(ctx, node)
		if err != nil {
			g.log.Warn("skipping unavailable node", "node", node, "error", err)
			skipped = append(skipped, fmt.Sprintf("%s: unavailable", node))
			continue
		}

		storageFree := map[string]float64{}
		storageTotal := map[string]float64{}
		if len(g.cfg.TargetStorages) > 0 {
			for _, resource := range storageResources {
				if resource.Type != "storage" {
					continue
				}
				if resource.Node != "" && resource.Node != node {
					continue
				}
				if !containsString(g.cfg.TargetStorages, resource.Storage) {
					continue
				}
				nodes, ok := storageNodes[resource.Storage]
				if !ok || !storageAllowsNode(nodes, node) {
					continue
				}
				if templateChoice.Resource.Node != node && !storageAllowsNode(nodes, templateChoice.Resource.Node) {
					continue
				}
				free := float64(resource.MaxDisk-resource.Disk) / 1024.0 / 1024.0 / 1024.0
				total := float64(resource.MaxDisk) / 1024.0 / 1024.0 / 1024.0
				if current, exists := storageFree[resource.Storage]; !exists || free > current {
					storageFree[resource.Storage] = free
				}
				if current, exists := storageTotal[resource.Storage]; !exists || total > current {
					storageTotal[resource.Storage] = total
				}
			}
			if len(storageFree) == 0 {
				skipped = append(skipped, fmt.Sprintf("%s: no eligible target storage from allowlist %s", node, strings.Join(g.cfg.TargetStorages, ",")))
				continue
			}
		}

		totalCPU := float64(status.CPUInfo.CPUs)
		totalMemoryMB := float64(status.Memory.Total) / 1024.0 / 1024.0
		nodes = append(nodes, nodePlanState{
			Name:            node,
			TemplateNode:    templateChoice.Resource.Node,
			TemplateVMID:    templateChoice.Resource.VMID,
			TemplateStorage: templateChoice.Storage,
			TotalMemoryMB:   totalMemoryMB,
			FreeMemoryMB:    float64(status.Memory.Total-status.Memory.Used) / 1024.0 / 1024.0,
			TotalCPUCores:   totalCPU,
			FreeCPUCores:    totalCPU - (status.CPU * totalCPU),
			StorageTotalGB:  storageTotal,
			StorageFreeGB:   storageFree,
		})
	}
	return nodes, skipped, nil
}

func (g *Group) buildCandidateNodes(states []nodePlanState) ([]scheduler.Node, []string) {
	nodes := make([]scheduler.Node, 0, len(states))
	skipped := make([]string, 0)
	for _, state := range states {
		targetStorage := ""
		freeDiskGB := 0.0
		if len(g.cfg.TargetStorages) > 0 {
			bestFree := -1.0
			for storage, free := range state.StorageFreeGB {
				if free > bestFree {
					bestFree = free
					targetStorage = storage
					freeDiskGB = free
				}
			}
			if targetStorage == "" {
				skipped = append(skipped, fmt.Sprintf("%s: no eligible target storage from allowlist %s", state.Name, strings.Join(g.cfg.TargetStorages, ",")))
				continue
			}
		}

		nodes = append(nodes, scheduler.Node{
			Name:          state.Name,
			TemplateNode:  state.TemplateNode,
			TemplateVMID:  state.TemplateVMID,
			TargetStorage: targetStorage,
			TotalMemoryMB: state.TotalMemoryMB,
			FreeMemoryMB:  state.FreeMemoryMB,
			TotalDiskGB:   state.StorageTotalGB[targetStorage],
			FreeDiskGB:    freeDiskGB,
			TotalCPUCores: state.TotalCPUCores,
			FreeCPUCores:  state.FreeCPUCores,
		})
	}
	return nodes, skipped
}

func stateTemplateStorage(states []nodePlanState, node string) string {
	for _, state := range states {
		if state.Name == node {
			return state.TemplateStorage
		}
	}
	return ""
}

func (g *Group) diagnosticNodes(states []nodePlanState) []scheduler.Node {
	nodes := make([]scheduler.Node, 0, len(states))
	for _, state := range states {
		targetStorage := ""
		freeDiskGB := 0.0
		for storage, free := range state.StorageFreeGB {
			if targetStorage == "" || free > freeDiskGB {
				targetStorage = storage
				freeDiskGB = free
			}
		}
		nodes = append(nodes, scheduler.Node{
			Name:          state.Name,
			TemplateNode:  state.TemplateNode,
			TemplateVMID:  state.TemplateVMID,
			TargetStorage: targetStorage,
			TotalMemoryMB: state.TotalMemoryMB,
			FreeMemoryMB:  state.FreeMemoryMB,
			TotalDiskGB:   state.StorageTotalGB[targetStorage],
			FreeDiskGB:    freeDiskGB,
			TotalCPUCores: state.TotalCPUCores,
			FreeCPUCores:  state.FreeCPUCores,
		})
	}
	return nodes
}

func (g *Group) reservePlannedResources(states []nodePlanState, node scheduler.Node, req scheduler.Requirement) {
	for i := range states {
		if states[i].Name != node.Name {
			continue
		}
		states[i].FreeMemoryMB -= req.MemoryMB
		states[i].FreeCPUCores -= req.CPUCores
		if node.TargetStorage != "" {
			states[i].StorageFreeGB[node.TargetStorage] -= req.DiskGB
		}
		return
	}
}

func (g *Group) applyPendingReservations(states []nodePlanState) {
	for i := range states {
		pending, ok := g.pendingByNode[states[i].Name]
		if !ok {
			continue
		}
		states[i].FreeMemoryMB -= pending.MemoryMB
		states[i].FreeCPUCores -= pending.CPUCores
		for storage, reserved := range pending.StorageGB {
			states[i].StorageFreeGB[storage] -= reserved
		}
	}
}

func (g *Group) registerPendingPlans(plans []provisionPlan) {
	for _, plan := range plans {
		pending := g.pendingByNode[plan.Node]
		pending.MemoryMB += plan.Requirement.MemoryMB
		pending.CPUCores += plan.Requirement.CPUCores
		if plan.TargetStorage != "" && plan.Requirement.DiskGB > 0 {
			if pending.StorageGB == nil {
				pending.StorageGB = map[string]float64{}
			}
			pending.StorageGB[plan.TargetStorage] += plan.Requirement.DiskGB
		}
		g.pendingByNode[plan.Node] = pending
		g.pendingVMIDs[plan.VMID] = struct{}{}
	}
}

func (g *Group) releasePendingPlan(plan provisionPlan) {
	g.mu.Lock()
	defer g.mu.Unlock()

	pending, ok := g.pendingByNode[plan.Node]
	if ok {
		pending.MemoryMB -= plan.Requirement.MemoryMB
		if pending.MemoryMB < 0 {
			pending.MemoryMB = 0
		}
		pending.CPUCores -= plan.Requirement.CPUCores
		if pending.CPUCores < 0 {
			pending.CPUCores = 0
		}
		if plan.TargetStorage != "" && pending.StorageGB != nil {
			pending.StorageGB[plan.TargetStorage] -= plan.Requirement.DiskGB
			if pending.StorageGB[plan.TargetStorage] <= 0 {
				delete(pending.StorageGB, plan.TargetStorage)
			}
		}
		if pending.MemoryMB == 0 && pending.CPUCores == 0 && len(pending.StorageGB) == 0 {
			delete(g.pendingByNode, plan.Node)
		} else {
			g.pendingByNode[plan.Node] = pending
		}
	}

	delete(g.pendingVMIDs, plan.VMID)
}

func storageAllowsNode(nodes map[string]struct{}, node string) bool {
	if len(nodes) == 0 {
		return true
	}
	_, ok := nodes[node]
	return ok
}

func indexStorageNodes(resources []proxmoxclient.ClusterResource) map[string]map[string]struct{} {
	nodesByStorage := make(map[string]map[string]struct{})
	for _, resource := range resources {
		if resource.Type != "storage" || resource.Storage == "" || resource.Node == "" {
			continue
		}
		nodes := nodesByStorage[resource.Storage]
		if nodes == nil {
			nodes = map[string]struct{}{}
			nodesByStorage[resource.Storage] = nodes
		}
		nodes[resource.Node] = struct{}{}
	}
	return nodesByStorage
}

func (g *Group) loadTemplateStorageNodes(ctx context.Context, templates []proxmoxclient.ClusterResource, storageResources []proxmoxclient.ClusterResource) (map[int]map[string]struct{}, error) {
	configs := make(map[int]map[string]struct{}, len(templates))
	nodesByStorage := indexStorageNodes(storageResources)
	for _, template := range templates {
		config, err := g.client.GetVMConfig(ctx, template.Node, template.VMID)
		if err != nil {
			return nil, fmt.Errorf("read template config %d: %w", template.VMID, err)
		}
		diskDevice, err := g.resolveDiskDevice(config)
		if err != nil {
			return nil, fmt.Errorf("resolve template disk device for %d: %w", template.VMID, err)
		}
		storageName, err := extractStorageName(config.DiskValue(diskDevice))
		if err != nil {
			return nil, fmt.Errorf("resolve template storage for %d: %w", template.VMID, err)
		}
		configs[template.VMID] = nodesByStorage[storageName]
	}
	return configs, nil
}

func (g *Group) resolveNodeTemplates(ctx context.Context, templates []proxmoxclient.ClusterResource, storageNodes map[int]map[string]struct{}) (map[string]templateChoice, proxmoxclient.ClusterResource, string, error) {
	choices := make(map[string]templateChoice, len(g.cfg.Nodes))
	localTemplateByNode := map[string]templateChoice{}
	var sizingTemplate proxmoxclient.ClusterResource
	var diskDevice string
	sizingSet := false

	for _, template := range templates {
		config, err := g.client.GetVMConfig(ctx, template.Node, template.VMID)
		if err != nil {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("read template config %d: %w", template.VMID, err)
		}
		resolvedDisk, err := g.resolveDiskDevice(config)
		if err != nil {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("resolve template disk device for %d: %w", template.VMID, err)
		}
		if !sizingSet {
			sizingTemplate = template
			diskDevice = resolvedDisk
			sizingSet = true
		} else if template.MaxMem != sizingTemplate.MaxMem || template.MaxDisk != sizingTemplate.MaxDisk || template.MaxCPU != sizingTemplate.MaxCPU {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("all template_vmids must have identical memory/cpu/disk sizing")
		} else if resolvedDisk != diskDevice {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("all template_vmids must use the same boot disk device")
		}

		currentDiskMB, ok := diskSizeMB(config.DiskValue(resolvedDisk))
		if g.cfg.VMDiskMB > 0 && ok && g.cfg.VMDiskMB < currentDiskMB {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("vm_disk_mb=%d is smaller than template disk %s size %dMB", g.cfg.VMDiskMB, resolvedDisk, currentDiskMB)
		}

		storageName, err := extractStorageName(config.DiskValue(resolvedDisk))
		if err != nil {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("resolve template storage for %d: %w", template.VMID, err)
		}
		choice := templateChoice{
			Resource:     template,
			Storage:      storageName,
			StorageNodes: storageNodes[template.VMID],
		}

		if _, exists := localTemplateByNode[template.Node]; exists {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("multiple template_vmids resolve to node %s", template.Node)
		}
		localTemplateByNode[template.Node] = choice
	}

	for _, node := range g.cfg.Nodes {
		if localChoice, ok := localTemplateByNode[node]; ok {
			choices[node] = localChoice
			continue
		}

		found := false
		for _, template := range templates {
			choice := localTemplateByNode[template.Node]
			if !containsString(g.cfg.TargetStorages, choice.Storage) {
				continue
			}
			if !storageAllowsNode(choice.StorageNodes, node) {
				continue
			}
			choices[node] = choice
			found = true
			break
		}
		if !found {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("node %s has no usable template: no local template and no shared template path via configured target_storages", node)
		}
	}

	if g.cfg.CloneMode == "linked" {
		for _, node := range g.cfg.Nodes {
			choice := choices[node]
			if choice.Resource.Node != node {
				return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("clone_mode=linked requires a local template on every configured node; node %s resolves to template on %s", node, choice.Resource.Node)
			}
			if len(g.cfg.TargetStorages) > 0 && !containsString(g.cfg.TargetStorages, choice.Storage) {
				return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("clone_mode=linked requires template storage %s for node %s to be included in target_storages", choice.Storage, node)
			}
		}
	}

	return choices, sizingTemplate, diskDevice, nil
}

func extractStorageName(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("disk value is empty")
	}
	storage, _, ok := strings.Cut(value, ":")
	if !ok || storage == "" {
		return "", fmt.Errorf("unable to parse storage from %q", value)
	}
	return storage, nil
}

func (g *Group) logEffectiveTopology() {
	nodes := append([]string(nil), g.cfg.Nodes...)
	for _, node := range nodes {
		choice, ok := g.templatesByNode[node]
		if !ok {
			continue
		}

		cloneMode := g.cfg.CloneMode
		targetStorage := "<proxmox-default>"
		if len(g.cfg.TargetStorages) > 0 {
			eligible := g.eligibleTargetStorages(node, choice)
			if len(eligible) > 0 {
				targetStorage = strings.Join(eligible, ",")
			} else {
				targetStorage = "<none>"
			}
		} else if g.cfg.CloneMode == "auto" && choice.Resource.Node == node {
			targetStorage = choice.Storage
		}

		g.log.Info(
			"validated node topology",
			"node", node,
			"template_vmid", choice.Resource.VMID,
			"template_node", choice.Resource.Node,
			"template_storage", choice.Storage,
			"clone_mode_effective", cloneMode,
			"target_storage_effective", targetStorage,
		)
	}
}

func (g *Group) eligibleTargetStorages(node string, choice templateChoice) []string {
	eligible := make([]string, 0, len(g.cfg.TargetStorages))
	for _, storage := range g.cfg.TargetStorages {
		nodes := g.storageNodes[storage]
		if !storageAllowsNode(nodes, node) {
			continue
		}
		if choice.Resource.Node != node && !storageAllowsNode(nodes, choice.Resource.Node) {
			continue
		}
		eligible = append(eligible, storage)
	}
	return eligible
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func (g *Group) provisionWorkerCount() int {
	workers := g.cloneLimiter.Capacity()
	if startWorkers := g.startLimiter.Capacity(); startWorkers > workers {
		workers = startWorkers
	}
	if workers <= 0 {
		return 1
	}
	return workers
}

func (g *Group) requiredMemoryMB() float64 {
	if g.cfg.VMMemoryMB > 0 {
		return float64(g.cfg.VMMemoryMB)
	}
	return float64(g.templateSizing.MaxMem) / 1024.0 / 1024.0
}

func (g *Group) requiredDiskGB() float64 {
	if g.cfg.VMDiskMB > 0 {
		return float64(g.cfg.VMDiskMB) / 1024.0
	}
	return float64(g.templateSizing.MaxDisk) / 1024.0 / 1024.0 / 1024.0
}

func (g *Group) requiredCPUCores() float64 {
	if g.cfg.VMCPUCores > 0 {
		return float64(g.cfg.VMCPUCores)
	}
	return g.templateSizing.MaxCPU
}

func (g *Group) resolveDiskDevice(config proxmoxclient.VMConfig) (string, error) {
	if g.cfg.VMDiskDevice != "" {
		if config.DiskValue(g.cfg.VMDiskDevice) == "" {
			return "", fmt.Errorf("vm_disk_device %q not present in template config", g.cfg.VMDiskDevice)
		}
		return g.cfg.VMDiskDevice, nil
	}
	if config.BootDisk != "" && config.DiskValue(config.BootDisk) != "" {
		return config.BootDisk, nil
	}
	for _, device := range []string{"scsi0", "virtio0", "sata0", "ide0"} {
		if config.DiskValue(device) != "" {
			return device, nil
		}
	}
	return "", fmt.Errorf("unable to determine template disk device; set vm_disk_device explicitly")
}

func diskSizeMB(value string) (int64, bool) {
	match := diskSizePattern.FindStringSubmatch(value)
	if len(match) != 3 {
		return 0, false
	}

	size, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, false
	}

	switch match[2] {
	case "K":
		return int64(size / 1024.0), true
	case "M":
		return int64(size), true
	case "G":
		return int64(size * 1024.0), true
	case "T":
		return int64(size * 1024.0 * 1024.0), true
	default:
		return 0, false
	}
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

func (g *Group) findTemplates(resources []proxmoxclient.ClusterResource) ([]proxmoxclient.ClusterResource, error) {
	byVMID := make(map[int]proxmoxclient.ClusterResource, len(resources))
	for _, resource := range resources {
		if resource.Type != "qemu" {
			continue
		}
		byVMID[resource.VMID] = resource
	}

	templates := make([]proxmoxclient.ClusterResource, 0, len(g.cfg.TemplateVMIDs))
	for _, vmid := range g.cfg.TemplateVMIDs {
		resource, ok := byVMID[vmid]
		if !ok {
			return nil, fmt.Errorf("template_vmids contains VMID %d which was not found", vmid)
		}
		if resource.Template != 1 {
			return nil, fmt.Errorf("template_vmid %d is not a template", vmid)
		}
		templates = append(templates, resource)
	}
	return templates, nil
}

func (g *Group) allocateVMIDs(resources []proxmoxclient.ClusterResource, count int) ([]int, error) {
	used := map[int]struct{}{}
	for _, resource := range resources {
		if resource.VMID > 0 {
			used[resource.VMID] = struct{}{}
		}
	}
	for vmid := range g.pendingVMIDs {
		used[vmid] = struct{}{}
	}
	vmids := make([]int, 0, count)
	for vmid := g.cfg.VMIDMin; vmid <= g.cfg.VMIDMax; vmid++ {
		if _, exists := used[vmid]; exists {
			continue
		}
		vmids = append(vmids, vmid)
		if len(vmids) == count {
			return vmids, nil
		}
	}
	if len(vmids) == 0 {
		return nil, fmt.Errorf("no free VMID in configured range")
	}
	return vmids, fmt.Errorf("only %d free VMIDs available in configured range", len(vmids))
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
