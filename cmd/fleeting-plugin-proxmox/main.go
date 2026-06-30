package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitlab.com/gitlab-org/fleeting/fleeting/plugin"
	proxmox "gitlab.com/gitlab-org/fleeting/plugins/proxmox"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "metrics-exporter" {
		runMetricsExporter(os.Args[2:])
		return
	}

	impl := &proxmox.InstanceGroup{}
	installSignalHandler(impl)
	plugin.Main(impl, proxmox.Version)
}

func runMetricsExporter(args []string) {
	fs := flag.NewFlagSet("metrics-exporter", flag.ExitOnError)
	listen := fs.String("listen", metrics.DefaultExporterListenAddress, "HTTP listen address for Prometheus scrapes")
	socket := fs.String("socket", metrics.DefaultExporterSocketPath, "Unix socket path for plugin metrics snapshots")
	path := fs.String("path", metrics.DefaultExporterMetricsPath, "HTTP path for Prometheus metrics")
	staleAfter := fs.Duration("stale-after", metrics.DefaultExporterStaleAfter, "Duration after which a silent plugin group is marked down")
	exceptionsFile := fs.String("exceptions-file", metrics.DefaultExceptionsFile, "Path to the aggregated human-readable problem summary")
	problemRetention := fs.Duration("problem-retention", metrics.DefaultProblemRetention, "How long resolved and recent problems remain visible")
	problemLimit := fs.Int("problem-limit", metrics.DefaultProblemLimit, "Maximum number of problems rendered in summaries")
	socketMode := fs.String("socket-mode", "0660", "Unix socket file mode")
	fs.Parse(args)

	mode, err := strconv.ParseUint(*socketMode, 8, 32)
	if err != nil {
		log.Fatalf("invalid --socket-mode: %v", err)
	}
	if *problemRetention <= 0 {
		log.Fatal("--problem-retention must be greater than zero")
	}
	if *problemLimit <= 0 {
		log.Fatal("--problem-limit must be greater than zero")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	cfg := metrics.ExporterConfig{
		ListenAddress:    *listen,
		SocketPath:       *socket,
		MetricsPath:      *path,
		StaleAfter:       *staleAfter,
		ExceptionsFile:   *exceptionsFile,
		ProblemRetention: *problemRetention,
		ProblemLimit:     *problemLimit,
		SocketMode:       os.FileMode(mode),
	}
	fmt.Fprintf(
		os.Stderr,
		"starting fleeting-plugin-proxmox metrics exporter listen=%s socket=%s path=%s exceptions=%s retention=%s\n",
		cfg.ListenAddress,
		cfg.SocketPath,
		cfg.MetricsPath,
		cfg.ExceptionsFile,
		cfg.ProblemRetention,
	)
	if err := metrics.RunExporter(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func installSignalHandler(impl *proxmox.InstanceGroup) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for range signals {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			_ = impl.Shutdown(ctx)
			cancel()
		}
	}()
}
