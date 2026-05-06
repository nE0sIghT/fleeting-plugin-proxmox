package metrics

type Identity struct {
	Cluster string `json:"cluster"`
	Pool    string `json:"pool"`
	Group   string `json:"group"`
}

type Snapshot struct {
	Version           int               `json:"version"`
	Identity          Identity          `json:"identity"`
	TimestampUnix     int64             `json:"timestamp_unix"`
	Up                bool              `json:"up"`
	LastScrapeSuccess bool              `json:"last_scrape_success"`
	LastError         string            `json:"last_error,omitempty"`
	Instances         map[string]int    `json:"instances,omitempty"`
	PendingInstances  int               `json:"pending_instances"`
	Nodes             []NodeSnapshot    `json:"nodes,omitempty"`
	Storages          []StorageSnapshot `json:"storages,omitempty"`
}

type NodeSnapshot struct {
	Node                       string  `json:"node"`
	TotalCPUCores              float64 `json:"total_cpu_cores"`
	RuntimeFreeCPUCores        float64 `json:"runtime_free_cpu_cores"`
	AllocatedCPUCores          float64 `json:"allocated_cpu_cores"`
	CPUAllocationLimitCores    float64 `json:"cpu_allocation_limit_cores"`
	TotalMemoryBytes           float64 `json:"total_memory_bytes"`
	RuntimeFreeMemoryBytes     float64 `json:"runtime_free_memory_bytes"`
	AllocatedMemoryBytes       float64 `json:"allocated_memory_bytes"`
	MemoryAllocationLimitBytes float64 `json:"memory_allocation_limit_bytes"`
}

type StorageSnapshot struct {
	Node       string  `json:"node"`
	Storage    string  `json:"storage"`
	TotalBytes float64 `json:"total_bytes"`
	FreeBytes  float64 `json:"free_bytes"`
}

func (i Identity) Key() string {
	return i.Cluster + "/" + i.Pool + "/" + i.Group
}
