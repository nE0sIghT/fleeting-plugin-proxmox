package operator

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"
)

type Options struct {
	Socket  string
	Cluster string
	Node    string
	JSON    bool
	Wait    bool
	Timeout time.Duration
}

func Parse(args []string) (string, Options, []metrics.NodeRef, error) {
	opts := Options{}
	fs := flag.NewFlagSet("fleeting-proxmoxctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.Socket, "socket", metrics.DefaultControlSocket, "Control socket")
	fs.StringVar(&opts.Cluster, "cluster", "", "Filter cluster")
	fs.StringVar(&opts.Node, "node", "", "Filter node")
	fs.BoolVar(&opts.JSON, "json", false, "JSON output")
	fs.BoolVar(&opts.Wait, "wait", false, "Wait until drained nodes have no working VMs or pending operations")
	fs.DurationVar(&opts.Timeout, "timeout", 2*time.Minute, "Command timeout")
	// The standard flag parser stops at a positional argument; accept flags both
	// before and after cluster/node operands.
	var flags, positionals []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			name, _, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
			f := fs.Lookup(name)
			if f != nil && !hasValue && name != "json" && name != "wait" {
				i++
				if i >= len(args) {
					return "", opts, nil, fmt.Errorf("missing value for %s", arg)
				}
				flags = append(flags, args[i])
			}
		} else {
			positionals = append(positionals, arg)
		}
	}
	if err := fs.Parse(flags); err != nil {
		return "", opts, nil, err
	}
	if opts.Timeout <= 0 {
		return "", opts, nil, fmt.Errorf("timeout must be positive")
	}
	action := "status"
	if len(positionals) > 0 {
		action = positionals[0]
		positionals = positionals[1:]
	}
	switch action {
	case "status", "nodes", "storages", "fleets", "instances", "problems", "drain", "undrain", "tui", "help":
	default:
		return "", opts, nil, fmt.Errorf("unknown command %q", action)
	}
	var nodes []metrics.NodeRef
	for _, value := range positionals {
		cluster, node, ok := strings.Cut(value, "/")
		if !ok || cluster == "" || node == "" || strings.Contains(node, "/") {
			return "", opts, nil, fmt.Errorf("expected cluster/node, got %q", value)
		}
		nodes = append(nodes, metrics.NodeRef{Cluster: cluster, Node: node})
	}
	if action == "drain" || action == "undrain" {
		if len(nodes) == 0 {
			return "", opts, nil, fmt.Errorf("%s requires one or more cluster/node operands", action)
		}
	} else if len(nodes) > 0 {
		return "", opts, nil, fmt.Errorf("%s does not accept node operands", action)
	}
	if opts.Wait && action != "drain" {
		return "", opts, nil, fmt.Errorf("--wait applies to drain")
	}
	return action, opts, nodes, nil
}

func Run(ctx context.Context, args []string, out io.Writer) error {
	action, opts, nodes, err := Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		action = "help"
		err = nil
	}
	if err != nil {
		return err
	}
	if action == "help" {
		_, err = fmt.Fprintln(out, "Usage: fleeting-proxmoxctl [status|nodes|storages|fleets|instances|problems|tui]\n       fleeting-proxmoxctl drain|undrain cluster/node [...] [--wait] [--timeout 30m]\nOptions: --socket PATH --cluster NAME --node NAME --json --timeout DURATION")
		return err
	}
	if action == "tui" {
		return RunTUI(ctx, opts)
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	request := metrics.ControlRequest{Action: "status"}
	if action == "drain" || action == "undrain" {
		request.Action = action
		request.Nodes = nodes
	}
	status, err := metrics.RequestControl(ctx, opts.Socket, request)
	if err != nil {
		return err
	}
	if len(nodes) > 0 {
		status, err = WaitNodes(ctx, opts.Socket, status, nodes, action == "drain", opts.Wait)
	}
	if opts.JSON {
		encodeErr := json.NewEncoder(out).Encode(status)
		if err == nil {
			err = encodeErr
		}
	} else {
		Render(out, action, status, opts)
	}
	return err
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return 130
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 2
	}
	return 1
}

func WaitNodes(ctx context.Context, socket string, status metrics.ControlStatus, nodes []metrics.NodeRef, drain, empty bool) (metrics.ControlStatus, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		complete := true
		for _, ref := range nodes {
			found := false
			for _, n := range status.Nodes {
				if n.NodeRef != ref {
					continue
				}
				found = true
				if n.Drained != drain {
					return status, fmt.Errorf("command superseded for %s", ref.Key())
				}
				if !n.Applied || (empty && !n.Empty) {
					complete = false
				}
			}
			if !found {
				complete = false
			}
		}
		if complete {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return status, fmt.Errorf("state saved; waiting for node confirmation: %w", ctx.Err())
		case <-ticker.C:
		}
		next, err := metrics.RequestControl(ctx, socket, metrics.ControlRequest{Action: "status"})
		if err != nil {
			return status, fmt.Errorf("state saved; cannot read confirmation: %w", err)
		}
		status = next
	}
}

func matches(opts Options, cluster, node string) bool {
	return (opts.Cluster == "" || opts.Cluster == cluster) && (opts.Node == "" || opts.Node == node)
}

func Render(out io.Writer, action string, status metrics.ControlStatus, opts Options) {
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintf(w, "LOCAL  %s  updated=%s  revision=%d\n", status.Problems.Status, time.Unix(status.GeneratedUnix, 0).UTC().Format(time.RFC3339), status.State.Revision)
	if action == "status" || action == "nodes" || action == "drain" || action == "undrain" {
		fmt.Fprintln(w, "CLUSTER\tNODE\tPLACEMENT\tCONFIRMED\tEMPTY\tVMs\tPENDING\tTEMPLATES")
		for _, n := range status.Nodes {
			if !matches(opts, n.Cluster, n.Node) {
				continue
			}
			placement := "enabled"
			if n.Drained {
				placement = "drained"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%t\t%t\t%d\t%d\t%d\n", safe(n.Cluster), safe(n.Node), placement, n.Applied, n.Empty, n.Instances, n.Pending, n.Templates)
			for _, f := range n.Awaiting {
				fmt.Fprintf(w, "\tawaiting fresh status: %s\n", safe(f))
			}
		}
	}
	if action == "status" || action == "fleets" || action == "nodes" || action == "storages" || action == "instances" {
		fmt.Fprintln(w, "\nFLEET\tCONNECTED\tAPPLIED\tFRESH\tAGE\tERROR")
		for _, f := range status.Fleets {
			if opts.Cluster != "" && opts.Cluster != f.Identity.Cluster {
				continue
			}
			age := "unknown"
			if f.ReceivedUnix > 0 {
				age = time.Since(time.Unix(f.ReceivedUnix, 0)).Round(time.Second).String()
			}
			fmt.Fprintf(w, "%s\t%t\t%t\t%t\t%s\t%s\n", safe(f.Identity.Key()), f.Connected, f.Applied, f.Fresh, age, safe(f.Snapshot.LastError))
		}
	}
	if action == "nodes" {
		fmt.Fprintln(w, "\nFLEET\tNODE\tCPU TOTAL/FREE/ALLOC/LIMIT/RESERVE\tRAM GiB TOTAL/FREE/ALLOC/LIMIT/RESERVE")
		for _, f := range status.Fleets {
			for _, n := range f.Snapshot.Nodes {
				if !matches(opts, f.Identity.Cluster, n.Node) {
					continue
				}
				fmt.Fprintf(w, "%s\t%s\t%.1f / %.1f / %.1f / %.1f / %.1f\t%.1f / %.1f / %.1f / %.1f / %.1f\n", safe(f.Identity.Key()), safe(n.Node), n.TotalCPUCores, n.RuntimeFreeCPUCores, n.AllocatedCPUCores, n.CPUAllocationLimitCores, n.ReservedCPUCores, gib(n.TotalMemoryBytes), gib(n.RuntimeFreeMemoryBytes), gib(n.AllocatedMemoryBytes), gib(n.MemoryAllocationLimitBytes), gib(n.ReservedMemoryBytes))
				for _, failure := range n.PlacementFailures {
					fmt.Fprintf(w, "\tplacement: %s\n", safe(failure))
				}
			}
			for node, message := range f.Snapshot.NodeErrors {
				if matches(opts, f.Identity.Cluster, node) {
					fmt.Fprintf(w, "%s\t%s\t%s\n", safe(f.Identity.Key()), safe(node), safe(message))
				}
			}
		}
	}
	if action == "storages" {
		fmt.Fprintln(w, "\nFLEET\tNODE\tSTORAGE\tTOTAL GiB\tFREE GiB\tRESERVED GiB\tSTATUS\tELIGIBLE\tSHARED")
		for _, f := range status.Fleets {
			storages := append([]metrics.StorageSnapshot{}, f.Snapshot.Storages...)
			if f.Snapshot.ObservedStorages != nil {
				storages = append([]metrics.StorageSnapshot{}, f.Snapshot.ObservedStorages...)
			}
			sort.Slice(storages, func(i, j int) bool {
				a, b := storages[i], storages[j]
				if a.Node != b.Node {
					return a.Node < b.Node
				}
				return a.Storage < b.Storage
			})
			for _, s := range storages {
				if !matches(opts, f.Identity.Cluster, s.Node) {
					continue
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%.1f\t%.1f\t%.1f\t%s\t%t\t%t\n", safe(f.Identity.Key()), safe(s.Node), safe(s.Storage), gib(s.TotalBytes), gib(s.FreeBytes), gib(s.ReservedBytes), safe(s.Status), s.Eligible, s.Shared)
			}
		}
	}
	if action == "instances" {
		fmt.Fprintln(w, "\nFLEET\tINSTANCE\tSTATE")
		for _, f := range status.Fleets {
			details := append([]metrics.InstanceSnapshot{}, f.Snapshot.InstanceDetails...)
			sort.Slice(details, func(i, j int) bool { return details[i].ID < details[j].ID })
			for _, i := range details {
				if matches(opts, f.Identity.Cluster, i.Node) {
					fmt.Fprintf(w, "%s\t%s\t%s\n", safe(f.Identity.Key()), safe(i.ID), safe(i.State))
				}
			}
		}
	}
	if action == "status" || action == "problems" {
		fmt.Fprintln(w, "\nSTATE\tFLEET\tNODE\tSTORAGE\tCODE\tCOUNT\tLAST\tERROR")
		for _, p := range status.Problems.Problems {
			if !matches(opts, p.Cluster, p.Node) {
				continue
			}
			fmt.Fprintf(w, "%s\t%s/%s/%s\t%s\t%s\t%s\t%d\t%s\t%s\n", p.State, safe(p.Cluster), safe(p.Pool), safe(p.Group), safe(p.Node), safe(p.Storage), safe(p.Code), p.Occurrences, time.Unix(p.LastSeenUnix, 0).UTC().Format(time.RFC3339), safe(p.Message))
		}
	}
}

func safe(value string) string {
	return strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, value)
}

func gib(value float64) float64 { return value / (1024 * 1024 * 1024) }
