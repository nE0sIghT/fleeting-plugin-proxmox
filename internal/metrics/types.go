package metrics

import "strings"

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
	ProblemEvents     []ProblemEvent    `json:"problem_events,omitempty"`
}

type ProblemState string

const (
	ProblemActive   ProblemState = "active"
	ProblemRecent   ProblemState = "recent"
	ProblemResolved ProblemState = "resolved"
)

type ProblemEvent struct {
	ID              string       `json:"id"`
	Code            string       `json:"code"`
	State           ProblemState `json:"state"`
	Severity        string       `json:"severity,omitempty"`
	Phase           string       `json:"phase,omitempty"`
	Node            string       `json:"node,omitempty"`
	Storage         string       `json:"storage,omitempty"`
	Instance        string       `json:"instance,omitempty"`
	OperationID     string       `json:"operation_id,omitempty"`
	Message         string       `json:"message,omitempty"`
	TimestampUnix   int64        `json:"timestamp_unix"`
	ActiveUntilUnix int64        `json:"active_until_unix,omitempty"`
	Occurrences     uint64       `json:"occurrences,omitempty"`
}

type ProblemReporter interface {
	ReportProblem(ProblemEvent)
}

func (p ProblemEvent) Key() string {
	return strings.Join([]string{p.Code, p.Phase, p.Node, p.Storage}, "\x00")
}

type NodeSnapshot struct {
	Node                              string  `json:"node"`
	TotalCPUCores                     float64 `json:"total_cpu_cores"`
	RuntimeFreeCPUCores               float64 `json:"runtime_free_cpu_cores"`
	ReservedCPUCores                  float64 `json:"reserved_cpu_cores"`
	AllocatedCPUCores                 float64 `json:"allocated_cpu_cores"`
	CPUAllocationLimitCores           float64 `json:"cpu_allocation_limit_cores"`
	PhysicalAllocationFreeCPUCores    float64 `json:"physical_allocation_free_cpu_cores"`
	TotalMemoryBytes                  float64 `json:"total_memory_bytes"`
	RuntimeFreeMemoryBytes            float64 `json:"runtime_free_memory_bytes"`
	ReservedMemoryBytes               float64 `json:"reserved_memory_bytes"`
	AllocatedMemoryBytes              float64 `json:"allocated_memory_bytes"`
	MemoryAllocationLimitBytes        float64 `json:"memory_allocation_limit_bytes"`
	PhysicalAllocationFreeMemoryBytes float64 `json:"physical_allocation_free_memory_bytes"`
}

type StorageSnapshot struct {
	Node          string  `json:"node"`
	Storage       string  `json:"storage"`
	TotalBytes    float64 `json:"total_bytes"`
	FreeBytes     float64 `json:"free_bytes"`
	ReservedBytes float64 `json:"reserved_bytes"`
}

func (i Identity) Key() string {
	return i.Cluster + "/" + i.Pool + "/" + i.Group
}
