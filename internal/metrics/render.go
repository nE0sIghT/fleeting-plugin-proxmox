package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

type storedSnapshot struct {
	Snapshot     Snapshot
	ReceivedUnix int64
}

func renderPrometheus(w io.Writer, snapshots []storedSnapshot, now time.Time, staleAfter time.Duration) {
	writeMetricHeader(w, "fleeting_proxmox_up", "Whether the plugin metrics reporter is currently up.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_last_update_timestamp_seconds", "Unix timestamp of the last metrics update received by the exporter.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_last_scrape_success", "Whether the plugin process collected its Proxmox metrics successfully.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_instances", "Managed instances by provider state.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_pending_instances", "Provisioning plans reserved locally by the plugin and not yet completed.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_total_cpu_cores", "Total CPU cores reported for the Proxmox node.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_runtime_free_cpu_cores", "Current free CPU cores estimated from Proxmox node utilization.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_reserved_cpu_cores", "Configured CPU reserve for the node after per-node policy resolution.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_allocated_cpu_cores", "Committed vCPU cores from running non-template QEMU VMs plus in-flight plugin reservations.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_cpu_allocation_limit_cores", "Configured committed vCPU limit for the node. Zero means disabled.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_allocation_free_cpu_cores", "Remaining committed vCPU allocation headroom. Zero when the allocation limit is disabled.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_physical_allocation_free_cpu_cores", "Remaining committed vCPU headroom against physical node CPU capacity.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_total_memory_bytes", "Total memory reported for the Proxmox node.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_runtime_free_memory_bytes", "Current free node memory reported by Proxmox.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_reserved_memory_bytes", "Configured memory reserve for the node after per-node policy resolution.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_allocated_memory_bytes", "Committed memory from running non-template QEMU VMs plus in-flight plugin reservations.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_memory_allocation_limit_bytes", "Configured committed memory limit for the node. Zero means disabled.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_allocation_free_memory_bytes", "Remaining committed memory allocation headroom. Zero when the allocation limit is disabled.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_node_physical_allocation_free_memory_bytes", "Remaining committed memory headroom against physical node memory capacity.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_storage_total_bytes", "Total capacity of an eligible target storage.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_storage_free_bytes", "Free capacity of an eligible target storage.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_storage_reserved_bytes", "Configured storage reserve for this node and storage after per-node policy resolution.", "gauge")

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Snapshot.Identity.Key() < snapshots[j].Snapshot.Identity.Key()
	})

	for _, stored := range snapshots {
		snapshot := stored.Snapshot
		stale := staleAfter > 0 && now.Sub(time.Unix(stored.ReceivedUnix, 0)) > staleAfter
		labels := groupLabels(snapshot.Identity)
		up := boolValue(snapshot.Up && !stale)
		success := boolValue(snapshot.LastScrapeSuccess && !stale)
		writeSample(w, "fleeting_proxmox_up", labels, up)
		writeSample(w, "fleeting_proxmox_last_update_timestamp_seconds", labels, float64(snapshot.TimestampUnix))
		writeSample(w, "fleeting_proxmox_last_scrape_success", labels, success)
		if stale || !snapshot.Up {
			continue
		}

		states := make([]string, 0, len(snapshot.Instances))
		for state := range snapshot.Instances {
			states = append(states, state)
		}
		sort.Strings(states)
		for _, state := range states {
			writeSample(w, "fleeting_proxmox_instances", appendLabel(labels, "state", state), float64(snapshot.Instances[state]))
		}
		writeSample(w, "fleeting_proxmox_pending_instances", labels, float64(snapshot.PendingInstances))

		nodes := append([]NodeSnapshot(nil), snapshot.Nodes...)
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].Node < nodes[j].Node
		})
		for _, node := range nodes {
			nodeLabels := appendLabel(labels, "node", node.Node)
			writeSample(w, "fleeting_proxmox_node_total_cpu_cores", nodeLabels, node.TotalCPUCores)
			writeSample(w, "fleeting_proxmox_node_runtime_free_cpu_cores", nodeLabels, node.RuntimeFreeCPUCores)
			writeSample(w, "fleeting_proxmox_node_reserved_cpu_cores", nodeLabels, node.ReservedCPUCores)
			writeSample(w, "fleeting_proxmox_node_allocated_cpu_cores", nodeLabels, node.AllocatedCPUCores)
			writeSample(w, "fleeting_proxmox_node_cpu_allocation_limit_cores", nodeLabels, node.CPUAllocationLimitCores)
			writeSample(w, "fleeting_proxmox_node_allocation_free_cpu_cores", nodeLabels, allocationFree(node.CPUAllocationLimitCores, node.AllocatedCPUCores))
			writeSample(w, "fleeting_proxmox_node_physical_allocation_free_cpu_cores", nodeLabels, node.PhysicalAllocationFreeCPUCores)
			writeSample(w, "fleeting_proxmox_node_total_memory_bytes", nodeLabels, node.TotalMemoryBytes)
			writeSample(w, "fleeting_proxmox_node_runtime_free_memory_bytes", nodeLabels, node.RuntimeFreeMemoryBytes)
			writeSample(w, "fleeting_proxmox_node_reserved_memory_bytes", nodeLabels, node.ReservedMemoryBytes)
			writeSample(w, "fleeting_proxmox_node_allocated_memory_bytes", nodeLabels, node.AllocatedMemoryBytes)
			writeSample(w, "fleeting_proxmox_node_memory_allocation_limit_bytes", nodeLabels, node.MemoryAllocationLimitBytes)
			writeSample(w, "fleeting_proxmox_node_allocation_free_memory_bytes", nodeLabels, allocationFree(node.MemoryAllocationLimitBytes, node.AllocatedMemoryBytes))
			writeSample(w, "fleeting_proxmox_node_physical_allocation_free_memory_bytes", nodeLabels, node.PhysicalAllocationFreeMemoryBytes)
		}

		storages := append([]StorageSnapshot(nil), snapshot.Storages...)
		sort.Slice(storages, func(i, j int) bool {
			if storages[i].Node != storages[j].Node {
				return storages[i].Node < storages[j].Node
			}
			return storages[i].Storage < storages[j].Storage
		})
		for _, storage := range storages {
			storageLabels := appendLabel(appendLabel(labels, "node", storage.Node), "storage", storage.Storage)
			writeSample(w, "fleeting_proxmox_storage_total_bytes", storageLabels, storage.TotalBytes)
			writeSample(w, "fleeting_proxmox_storage_free_bytes", storageLabels, storage.FreeBytes)
			writeSample(w, "fleeting_proxmox_storage_reserved_bytes", storageLabels, storage.ReservedBytes)
		}
	}
}

func renderProblemPrometheus(w io.Writer, status ProblemStatus) {
	writeMetricHeader(w, "fleeting_proxmox_problem_active", "Whether a grouped plugin problem is currently active.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_problem_recent", "Whether a grouped plugin problem occurred within the configured retention period.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_problem_occurrences", "Number of occurrences aggregated for a plugin problem within the exporter retention period.", "gauge")
	writeMetricHeader(w, "fleeting_proxmox_problem_last_seen_timestamp_seconds", "Unix timestamp of the last occurrence of a grouped plugin problem.", "gauge")

	for _, problem := range status.Problems {
		labels := []label{
			{name: "cluster", value: problem.Cluster},
			{name: "pool", value: problem.Pool},
			{name: "group", value: problem.Group},
			{name: "code", value: problem.Code},
			{name: "phase", value: problem.Phase},
			{name: "node", value: problem.Node},
			{name: "storage", value: problem.Storage},
		}
		writeSample(w, "fleeting_proxmox_problem_active", labels, boolValue(problem.State == ProblemActive))
		writeSample(w, "fleeting_proxmox_problem_recent", labels, boolValue(problem.State == ProblemRecent))
		writeSample(w, "fleeting_proxmox_problem_occurrences", labels, float64(problem.Occurrences))
		writeSample(w, "fleeting_proxmox_problem_last_seen_timestamp_seconds", labels, float64(problem.LastSeenUnix))
	}
}

func writeMetricHeader(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
}

func writeSample(w io.Writer, name string, labels []label, value float64) {
	fmt.Fprintf(w, "%s{%s} %s\n", name, formatLabels(labels), strconv.FormatFloat(value, 'f', -1, 64))
}

type label struct {
	name  string
	value string
}

func groupLabels(identity Identity) []label {
	return []label{
		{name: "cluster", value: identity.Cluster},
		{name: "pool", value: identity.Pool},
		{name: "group", value: identity.Group},
	}
}

func appendLabel(labels []label, name, value string) []label {
	out := make([]label, 0, len(labels)+1)
	out = append(out, labels...)
	out = append(out, label{name: name, value: value})
	return out
}

func formatLabels(labels []label) string {
	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		parts = append(parts, label.name+`="`+escapeLabelValue(label.value)+`"`)
	}
	return strings.Join(parts, ",")
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func allocationFree(limit, allocated float64) float64 {
	if limit <= 0 {
		return 0
	}
	free := limit - allocated
	if free < 0 {
		return 0
	}
	return free
}
