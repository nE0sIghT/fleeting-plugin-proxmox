package metrics

const (
	DefaultControlSocket    = "/run/fleeting-plugin-proxmox/control.sock"
	DefaultControlStateFile = "/var/lib/fleeting-plugin-proxmox/control.json"
)

type NodeRef struct {
	Cluster string `json:"cluster"`
	Node    string `json:"node"`
}

func (n NodeRef) Key() string { return n.Cluster + "/" + n.Node }

type DrainState struct {
	Epoch    string    `json:"epoch"`
	Revision uint64    `json:"revision"`
	Nodes    []NodeRef `json:"nodes"`
}

type controlMessage struct {
	Type  string     `json:"type"`
	State DrainState `json:"state"`
	Error string     `json:"error,omitempty"`
}

type ControlRequest struct {
	Version int       `json:"version"`
	Action  string    `json:"action"`
	Nodes   []NodeRef `json:"nodes,omitempty"`
}

type ControlResponse struct {
	Error  string        `json:"error,omitempty"`
	Status ControlStatus `json:"status"`
}

type ControlStatus struct {
	State         DrainState    `json:"state"`
	GeneratedUnix int64         `json:"generated_unix"`
	Fleets        []FleetStatus `json:"fleets"`
	Nodes         []DrainStatus `json:"nodes"`
	Problems      ProblemStatus `json:"problems"`
}

type FleetStatus struct {
	Identity        Identity `json:"identity"`
	Session         string   `json:"session"`
	Connected       bool     `json:"connected"`
	Applied         bool     `json:"applied"`
	Fresh           bool     `json:"fresh"`
	ReceivedUnix    int64    `json:"received_unix"`
	ConfiguredNodes []string `json:"configured_nodes"`
	Snapshot        Snapshot `json:"snapshot"`
}

type DrainStatus struct {
	NodeRef
	Drained   bool     `json:"drained"`
	Applied   bool     `json:"applied"`
	Empty     bool     `json:"empty"`
	Instances int      `json:"instances"`
	Pending   int      `json:"pending"`
	Templates int      `json:"templates"`
	Awaiting  []string `json:"awaiting,omitempty"`
}

type InstanceSnapshot struct {
	ID    string `json:"id"`
	Node  string `json:"node"`
	State string `json:"state"`
}
