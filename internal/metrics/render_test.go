package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRenderPrometheusSnapshot(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	renderPrometheus(&out, []storedSnapshot{
		{
			Snapshot: Snapshot{
				Version:           1,
				Identity:          Identity{Cluster: "prod", Pool: "ci", Group: "arm64"},
				TimestampUnix:     100,
				Up:                true,
				LastScrapeSuccess: true,
				Instances: map[string]int{
					"running":  2,
					"creating": 1,
				},
				PendingInstances: 1,
				Nodes: []NodeSnapshot{
					{
						Node:                       "pve01",
						TotalCPUCores:              64,
						RuntimeFreeCPUCores:        40,
						AllocatedCPUCores:          60,
						CPUAllocationLimitCores:    64,
						TotalMemoryBytes:           128 * 1024 * 1024 * 1024,
						RuntimeFreeMemoryBytes:     64 * 1024 * 1024 * 1024,
						AllocatedMemoryBytes:       96 * 1024 * 1024 * 1024,
						MemoryAllocationLimitBytes: 128 * 1024 * 1024 * 1024,
					},
				},
				Storages: []StorageSnapshot{
					{Node: "pve01", Storage: "nvme0", TotalBytes: 1000, FreeBytes: 250},
				},
			},
			ReceivedUnix: 100,
		},
	}, time.Unix(110, 0), time.Minute)

	text := out.String()
	require.Contains(t, text, `fleeting_proxmox_up{cluster="prod",pool="ci",group="arm64"} 1`)
	require.Contains(t, text, `fleeting_proxmox_instances{cluster="prod",pool="ci",group="arm64",state="running"} 2`)
	require.Contains(t, text, `fleeting_proxmox_pending_instances{cluster="prod",pool="ci",group="arm64"} 1`)
	require.Contains(t, text, `fleeting_proxmox_node_allocation_free_cpu_cores{cluster="prod",pool="ci",group="arm64",node="pve01"} 4`)
	require.Contains(t, text, `fleeting_proxmox_storage_free_bytes{cluster="prod",pool="ci",group="arm64",node="pve01",storage="nvme0"} 250`)
}

func TestRenderPrometheusStaleSnapshotSuppressesResourceGauges(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	renderPrometheus(&out, []storedSnapshot{
		{
			Snapshot: Snapshot{
				Identity:          Identity{Cluster: "prod", Pool: "ci", Group: "arm64"},
				TimestampUnix:     100,
				Up:                true,
				LastScrapeSuccess: true,
				Nodes:             []NodeSnapshot{{Node: "pve01", TotalCPUCores: 64}},
			},
			ReceivedUnix: 100,
		},
	}, time.Unix(200, 0), time.Minute)

	text := out.String()
	require.Contains(t, text, `fleeting_proxmox_up{cluster="prod",pool="ci",group="arm64"} 0`)
	require.NotContains(t, text, `node="pve01"`)
}

func TestEscapeLabelValue(t *testing.T) {
	t.Parallel()

	require.Equal(t, `a\\b\"c\n`, escapeLabelValue("a\\b\"c\n"))
	require.False(t, strings.Contains(formatLabels([]label{{name: "x", value: "a\nb"}}), "\n"))
}
