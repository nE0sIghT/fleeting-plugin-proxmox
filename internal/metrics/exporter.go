package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	DefaultExporterListenAddress = "127.0.0.1:9252"
	DefaultExporterSocketPath    = "/run/fleeting-plugin-proxmox/metrics.sock"
	DefaultExporterMetricsPath   = "/metrics"
	DefaultExporterStaleAfter    = time.Minute
	DefaultSocketMode            = 0o660
)

type ExporterConfig struct {
	ListenAddress string
	SocketPath    string
	MetricsPath   string
	StaleAfter    time.Duration
	SocketMode    os.FileMode
}

type Exporter struct {
	cfg ExporterConfig

	mu        sync.Mutex
	snapshots map[string]storedSnapshot
}

func RunExporter(ctx context.Context, cfg ExporterConfig) error {
	exporter := NewExporter(cfg)
	return exporter.Run(ctx)
}

func NewExporter(cfg ExporterConfig) *Exporter {
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = DefaultExporterListenAddress
	}
	if cfg.SocketPath == "" {
		cfg.SocketPath = DefaultExporterSocketPath
	}
	if cfg.MetricsPath == "" {
		cfg.MetricsPath = DefaultExporterMetricsPath
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = DefaultExporterStaleAfter
	}
	if cfg.SocketMode == 0 {
		cfg.SocketMode = DefaultSocketMode
	}
	return &Exporter{
		cfg:       cfg,
		snapshots: map[string]storedSnapshot{},
	}
}

func (e *Exporter) Run(ctx context.Context) error {
	listener, err := listenUnix(e.cfg.SocketPath, e.cfg.SocketMode)
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(e.cfg.SocketPath)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc(e.cfg.MetricsPath, e.handleMetrics)
	server := &http.Server{
		Addr:              e.cfg.ListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errs := make(chan error, 2)
	go func() {
		errs <- e.serveUnix(listener)
	}()
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errs <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = listener.Close()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-errs:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = listener.Close()
		_ = server.Shutdown(shutdownCtx)
		return err
	}
}

func listenUnix(path string, mode os.FileMode) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create metrics socket directory: %w", err)
	}
	if _, err := os.Stat(path); err == nil {
		conn, dialErr := net.DialTimeout("unix", path, time.Second)
		if dialErr == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("metrics socket %s is already in use", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale metrics socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat metrics socket: %w", err)
	}

	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on metrics socket: %w", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("chmod metrics socket: %w", err)
	}
	return listener, nil
}

func (e *Exporter) serveUnix(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go e.handleUnixConn(conn)
	}
}

func (e *Exporter) handleUnixConn(conn net.Conn) {
	defer conn.Close()

	var snapshot Snapshot
	decoder := json.NewDecoder(io.LimitReader(conn, 1<<20))
	if err := decoder.Decode(&snapshot); err != nil {
		return
	}
	if snapshot.Version != 1 || snapshot.Identity.Cluster == "" || snapshot.Identity.Pool == "" || snapshot.Identity.Group == "" {
		return
	}
	if snapshot.TimestampUnix == 0 {
		snapshot.TimestampUnix = time.Now().Unix()
	}

	e.mu.Lock()
	e.snapshots[snapshot.Identity.Key()] = storedSnapshot{
		Snapshot:     snapshot,
		ReceivedUnix: time.Now().Unix(),
	}
	e.mu.Unlock()
}

func (e *Exporter) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	e.mu.Lock()
	snapshots := make([]storedSnapshot, 0, len(e.snapshots))
	for _, snapshot := range e.snapshots {
		snapshots = append(snapshots, snapshot)
	}
	e.mu.Unlock()

	renderPrometheus(w, snapshots, time.Now(), e.cfg.StaleAfter)
}
