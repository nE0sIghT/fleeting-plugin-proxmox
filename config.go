package proxmox

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

const (
	defaultCloneMode         = "auto"
	defaultNetworkMode       = "static"
	defaultTaskPollInterval  = 2 * time.Second
	defaultCloneTimeout      = 10 * time.Minute
	defaultStartTimeout      = 5 * time.Minute
	defaultShutdownTimeout   = 2 * time.Minute
	defaultAgentTimeout      = 3 * time.Minute
	defaultIPReuseCooldown   = 10 * time.Minute
	defaultCloudInitIFace    = "ipconfig0"
	defaultStateDir          = "/var/lib/fleeting-plugin-proxmox"
	defaultStateFileBasename = "state.json"
)

type vmidRange struct {
	Min int
	Max int
}

type pluginConfig struct {
	APIURL                string        `json:"api_url"`
	TokenID               string        `json:"token_id"`
	TokenSecret           string        `json:"token_secret"`
	TLSCAFile             string        `json:"tls_ca_file"`
	TLSInsecureSkipVerify bool          `json:"tls_insecure_skip_verify"`
	ClusterName           string        `json:"cluster_name"`
	Pool                  string        `json:"pool"`
	TemplateVMIDs         []int         `json:"template_vmids"`
	NamePrefix            string        `json:"name_prefix"`
	VMIDRange             string        `json:"vmid_range"`
	Nodes                 LaxStringList `json:"nodes"`

	CloneMode      string        `json:"clone_mode"`
	TargetStorages LaxStringList `json:"target_storages"`
	CloneSnapshot  string        `json:"clone_snapshot"`
	VMMemoryMB     int64         `json:"vm_memory_mb"`
	VMCPUCores     int           `json:"vm_cpu_cores"`
	VMDiskMB       int64         `json:"vm_disk_mb"`
	VMDiskDevice   string        `json:"vm_disk_device"`

	NodeReserveMemoryMB      int64  `json:"node_reserve_memory_mb"`
	NodeReserveMemoryPercent int    `json:"node_reserve_memory_percent"`
	NodeReserveCPUCores      int    `json:"node_reserve_cpu_cores"`
	NodeReserveCPUPercent    int    `json:"node_reserve_cpu_percent"`
	NodeReserveDiskGB        int64  `json:"node_reserve_disk_gb"`
	NodeReserveDiskPercent   int    `json:"node_reserve_disk_percent"`
	Scheduler                string `json:"scheduler"`

	MaxParallelClones  int `json:"max_parallel_clones"`
	MaxParallelStarts  int `json:"max_parallel_starts"`
	MaxParallelDeletes int `json:"max_parallel_deletes"`

	TaskPollInterval string `json:"task_poll_interval"`
	CloneTimeout     string `json:"clone_timeout"`
	StartTimeout     string `json:"start_timeout"`
	ShutdownTimeout  string `json:"shutdown_timeout"`

	CloudInitEnabled   bool          `json:"cloud_init_enabled"`
	CloudInitInterface string        `json:"cloud_init_interface"`
	NetworkMode        string        `json:"network_mode"`
	CIUser             string        `json:"ci_user"`
	CISSHKeys          LaxStringList `json:"ci_ssh_keys"`
	NameServers        LaxStringList `json:"nameserver"`
	SearchDomain       string        `json:"searchdomain"`

	IPPoolNetwork       string        `json:"ip_pool_network"`
	IPPoolGateway       string        `json:"ip_pool_gateway"`
	IPPoolRanges        LaxStringList `json:"ip_pool_ranges"`
	IPPoolExclude       LaxStringList `json:"ip_pool_exclude"`
	IPPoolReuseCooldown string        `json:"ip_pool_reuse_cooldown"`
	StateFile           string        `json:"state_file"`

	AgentRequired bool   `json:"agent_required"`
	AgentTimeout  string `json:"agent_timeout"`
	PreferIPv6    bool   `json:"prefer_ipv6"`

	Tags                LaxStringList `json:"tags"`
	DescriptionTemplate string        `json:"description_template"`

	parsedVMIDRange       vmidRange
	parsedTaskPoll        time.Duration
	parsedCloneTimeout    time.Duration
	parsedStartTimeout    time.Duration
	parsedShutdownTimeout time.Duration
	parsedAgentTimeout    time.Duration
	parsedIPReuseCooldown time.Duration
	parsedPoolPrefix      netip.Prefix
	parsedGateway         netip.Addr
}

func (g *InstanceGroup) config() *pluginConfig {
	return &g.pluginConfig
}

func (c *pluginConfig) applyDefaults(settings provider.Settings) {
	if c.ClusterName == "" {
		c.ClusterName = "default"
	}
	if c.CloneMode == "" {
		c.CloneMode = defaultCloneMode
	}
	if c.NetworkMode == "" {
		c.NetworkMode = defaultNetworkMode
	}
	if c.MaxParallelClones <= 0 {
		c.MaxParallelClones = 2
	}
	if c.MaxParallelStarts <= 0 {
		c.MaxParallelStarts = 4
	}
	if c.MaxParallelDeletes <= 0 {
		c.MaxParallelDeletes = 2
	}
	if c.TaskPollInterval == "" {
		c.TaskPollInterval = defaultTaskPollInterval.String()
	}
	if c.Scheduler == "" {
		c.Scheduler = "balanced"
	}
	if c.CloneTimeout == "" {
		c.CloneTimeout = defaultCloneTimeout.String()
	}
	if c.StartTimeout == "" {
		c.StartTimeout = defaultStartTimeout.String()
	}
	if c.ShutdownTimeout == "" {
		c.ShutdownTimeout = defaultShutdownTimeout.String()
	}
	if c.IPPoolReuseCooldown == "" {
		c.IPPoolReuseCooldown = defaultIPReuseCooldown.String()
	}
	if c.CloudInitInterface == "" {
		c.CloudInitInterface = defaultCloudInitIFace
	}
	if c.StateFile == "" {
		c.StateFile = filepath.Join(defaultStateDir, defaultStateFileName(c.ClusterName, c.Pool, c.NamePrefix))
	}
	if !c.CloudInitEnabled {
		c.CloudInitEnabled = true
	}
	if !c.AgentRequired {
		c.AgentRequired = true
	}
	if c.AgentTimeout == "" {
		c.AgentTimeout = defaultAgentTimeout.String()
	}
	if settings.Protocol == "" {
		settings.Protocol = provider.ProtocolSSH
	}
}

func (c *pluginConfig) validate(settings provider.Settings) error {
	c.applyEnv()
	c.applyDefaults(settings)

	var errs []error

	if c.APIURL == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: api_url"))
	}
	if c.TokenID == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: token_id"))
	}
	if c.TokenSecret == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: token_secret"))
	}
	if c.Pool == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: pool"))
	}
	if len(c.TemplateVMIDs) == 0 {
		errs = append(errs, fmt.Errorf("missing required plugin config: template_vmids"))
	}
	if c.NamePrefix == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: name_prefix"))
	}
	if len(c.Nodes) == 0 {
		errs = append(errs, fmt.Errorf("missing required plugin config: nodes"))
	}
	if c.VMIDRange == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: vmid_range"))
	}
	if settings.Protocol != "" && settings.Protocol != provider.ProtocolSSH {
		errs = append(errs, fmt.Errorf("unsupported connector protocol: %s", settings.Protocol))
	}
	if settings.OS != "" && settings.OS != "linux" {
		errs = append(errs, fmt.Errorf("unsupported connector OS: %s", settings.OS))
	}
	if c.CloneMode != "auto" && c.CloneMode != "linked" && c.CloneMode != "full" {
		errs = append(errs, fmt.Errorf("invalid clone_mode: %s", c.CloneMode))
	}
	if c.VMMemoryMB < 0 {
		errs = append(errs, fmt.Errorf("vm_memory_mb must be >= 0"))
	}
	if c.VMCPUCores < 0 {
		errs = append(errs, fmt.Errorf("vm_cpu_cores must be >= 0"))
	}
	if c.VMDiskMB < 0 {
		errs = append(errs, fmt.Errorf("vm_disk_mb must be >= 0"))
	}
	if c.NodeReserveMemoryPercent < 0 || c.NodeReserveMemoryPercent > 100 {
		errs = append(errs, fmt.Errorf("node_reserve_memory_percent must be between 0 and 100"))
	}
	if c.NodeReserveCPUPercent < 0 || c.NodeReserveCPUPercent > 100 {
		errs = append(errs, fmt.Errorf("node_reserve_cpu_percent must be between 0 and 100"))
	}
	if c.NodeReserveDiskPercent < 0 || c.NodeReserveDiskPercent > 100 {
		errs = append(errs, fmt.Errorf("node_reserve_disk_percent must be between 0 and 100"))
	}
	if c.NetworkMode != "static" && c.NetworkMode != "dhcp" {
		errs = append(errs, fmt.Errorf("invalid network_mode: %s", c.NetworkMode))
	}
	if c.Scheduler != "balanced" && c.Scheduler != "most_free_ram" && c.Scheduler != "most_free_cpu" && c.Scheduler != "round_robin" {
		errs = append(errs, fmt.Errorf("invalid scheduler: %s", c.Scheduler))
	}
	if c.CloudInitInterface != "ipconfig0" {
		errs = append(errs, fmt.Errorf("cloud_init_interface must be ipconfig0 in v1"))
	}
	if !c.CloudInitEnabled {
		errs = append(errs, fmt.Errorf("cloud_init_enabled=false is unsupported in v1"))
	}
	if c.NetworkMode == "dhcp" && !c.AgentRequired {
		errs = append(errs, fmt.Errorf("network_mode=dhcp requires agent_required=true"))
	}
	if c.PreferIPv6 {
		errs = append(errs, fmt.Errorf("prefer_ipv6 is unsupported in v1"))
	}

	c.parsedVMIDRange = parseVMIDRange(c.VMIDRange, &errs)

	seenTemplateVMIDs := map[int]struct{}{}
	for _, vmid := range c.TemplateVMIDs {
		if vmid <= 0 {
			errs = append(errs, fmt.Errorf("template_vmids contains invalid VMID %d", vmid))
			continue
		}
		if _, exists := seenTemplateVMIDs[vmid]; exists {
			errs = append(errs, fmt.Errorf("template_vmids contains duplicate VMID %d", vmid))
			continue
		}
		seenTemplateVMIDs[vmid] = struct{}{}
	}

	c.parsedTaskPoll = parseDurationField("task_poll_interval", c.TaskPollInterval, &errs)
	c.parsedCloneTimeout = parseDurationField("clone_timeout", c.CloneTimeout, &errs)
	c.parsedStartTimeout = parseDurationField("start_timeout", c.StartTimeout, &errs)
	c.parsedShutdownTimeout = parseDurationField("shutdown_timeout", c.ShutdownTimeout, &errs)
	c.parsedAgentTimeout = parseDurationField("agent_timeout", c.AgentTimeout, &errs)
	c.parsedIPReuseCooldown = parseDurationField("ip_pool_reuse_cooldown", c.IPPoolReuseCooldown, &errs)

	if c.NetworkMode == "static" {
		if c.IPPoolNetwork == "" {
			errs = append(errs, fmt.Errorf("missing required plugin config for static network_mode: ip_pool_network"))
		}
		if c.IPPoolGateway == "" {
			errs = append(errs, fmt.Errorf("missing required plugin config for static network_mode: ip_pool_gateway"))
		}

		prefix, err := netip.ParsePrefix(c.IPPoolNetwork)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid ip_pool_network: %w", err))
		} else if !prefix.Addr().Is4() {
			errs = append(errs, fmt.Errorf("ip_pool_network must be IPv4"))
		} else {
			c.parsedPoolPrefix = prefix.Masked()
		}

		gateway, err := netip.ParseAddr(c.IPPoolGateway)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid ip_pool_gateway: %w", err))
		} else if !gateway.Is4() {
			errs = append(errs, fmt.Errorf("ip_pool_gateway must be IPv4"))
		} else {
			c.parsedGateway = gateway
		}

		for _, value := range append(append(LaxStringList{}, c.IPPoolExclude...), c.flattenRanges()...) {
			if strings.Contains(value, "-") {
				if _, _, err := parseAddrRange(value); err != nil {
					errs = append(errs, fmt.Errorf("invalid range %q: %w", value, err))
				}
				continue
			}

			addr, err := netip.ParseAddr(value)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid IPv4 address %q: %w", value, err))
				continue
			}
			if c.parsedPoolPrefix.IsValid() && !c.parsedPoolPrefix.Contains(addr) {
				errs = append(errs, fmt.Errorf("address %q is outside ip_pool_network", value))
			}
		}
	}

	if err := os.MkdirAll(filepath.Dir(c.StateFile), 0o755); err != nil {
		errs = append(errs, fmt.Errorf("prepare state_file directory: %w", err))
	}

	return errors.Join(errs...)
}

func (c *pluginConfig) applyEnv() {
	overrideEnv(&c.APIURL, "PROXMOX_API_URL")
	overrideEnv(&c.TokenID, "PROXMOX_TOKEN_ID")
	overrideEnv(&c.TokenSecret, "PROXMOX_TOKEN_SECRET")
	overrideEnv(&c.TLSCAFile, "PROXMOX_TLS_CA_FILE")
	overrideEnv(&c.StateFile, "PROXMOX_STATE_FILE")
}

func (c *pluginConfig) mandatoryTags() []string {
	tagGroup := sanitizeTag("fleeting-group-" + c.NamePrefix)
	return append([]string{"managed-by-fleeting-plugin-proxmox", tagGroup}, c.Tags...)
}

func (c *pluginConfig) flattenRanges() []string {
	return slices.Clone(c.IPPoolRanges)
}

func parseVMIDRange(value string, errs *[]error) vmidRange {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		*errs = append(*errs, fmt.Errorf("invalid vmid_range: %s", value))
		return vmidRange{}
	}

	var r vmidRange
	if _, err := fmt.Sscanf(value, "%d-%d", &r.Min, &r.Max); err != nil || r.Min <= 0 || r.Max < r.Min {
		*errs = append(*errs, fmt.Errorf("invalid vmid_range: %s", value))
	}

	return r
}

func parseDurationField(name, value string, errs *[]error) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("invalid %s: %w", name, err))
	}
	return d
}

func defaultStateFileName(clusterName, pool, namePrefix string) string {
	parts := []string{
		sanitizeTag(clusterName),
		sanitizeTag(pool),
		sanitizeTag(namePrefix),
	}
	filtered := parts[:0]
	for _, part := range parts {
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return defaultStateFileBasename
	}
	return strings.Join(append(filtered, defaultStateFileBasename), "-")
}

func overrideEnv(dst *string, key string) {
	if value := os.Getenv(key); value != "" {
		*dst = value
	}
}

func sanitizeTag(v string) string {
	v = strings.ToLower(v)
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('-')
	}
	return strings.Trim(b.String(), "-")
}

func parseAddrRange(value string) (netip.Addr, netip.Addr, error) {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("range must be start-end")
	}

	start, err := netip.ParseAddr(parts[0])
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	end, err := netip.ParseAddr(parts[1])
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	if !start.Is4() || !end.Is4() {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("only IPv4 ranges are supported")
	}
	if start.Compare(end) > 0 {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("range start must be <= end")
	}
	return start, end, nil
}
