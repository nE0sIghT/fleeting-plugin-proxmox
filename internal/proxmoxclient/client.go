package proxmoxclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

var ErrNotFound = errors.New("resource not found")

type Config struct {
	BaseURL            string
	TokenID            string
	TokenSecret        string
	TLSCAFile          string
	InsecureSkipVerify bool
}

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	authHeader string
}

type envelope[T any] struct {
	Data T `json:"data"`
}

type Version struct {
	Release string `json:"release"`
}

type ClusterResource struct {
	ID       string  `json:"id"`
	Type     string  `json:"type"`
	Pool     string  `json:"pool"`
	Node     string  `json:"node"`
	Name     string  `json:"name"`
	Storage  string  `json:"storage"`
	Tags     string  `json:"tags"`
	Status   string  `json:"status"`
	Template int     `json:"template"`
	Shared   int     `json:"shared"`
	VMID     int     `json:"vmid"`
	Disk     int64   `json:"disk"`
	MaxMem   int64   `json:"maxmem"`
	MaxDisk  int64   `json:"maxdisk"`
	MaxCPU   float64 `json:"maxcpu"`
	CPU      float64 `json:"cpu"`
}

type NodeStatus struct {
	CPU     float64 `json:"cpu"`
	CPUInfo struct {
		CPUs int `json:"cpus"`
	} `json:"cpuinfo"`
	Memory struct {
		Used  int64 `json:"used"`
		Total int64 `json:"total"`
	} `json:"memory"`
	RootFS struct {
		Used  int64 `json:"used"`
		Total int64 `json:"total"`
	} `json:"rootfs"`
}

type VMConfig struct {
	Name         string `json:"name"`
	Pool         string `json:"pool"`
	Tags         string `json:"tags"`
	Description  string `json:"description"`
	IPConfig0    string `json:"ipconfig0"`
	SSHKeys      string `json:"sshkeys"`
	CIUser       string `json:"ciuser"`
	NameServer   string `json:"nameserver"`
	SearchDomain string `json:"searchdomain"`
	Agent        string `json:"agent"`
	Template     int    `json:"template"`
}

type VMStatus struct {
	Status    string `json:"status"`
	QMPStatus string `json:"qmpstatus"`
}

type TaskStatus struct {
	Status     string `json:"status"`
	ExitStatus string `json:"exitstatus"`
}

type PoolInfo struct {
	PoolID string `json:"poolid"`
}

type AgentInterface struct {
	Name        string           `json:"name"`
	IPAddresses []AgentIPAddress `json:"ip-addresses"`
}

type AgentIPAddress struct {
	IPAddress string `json:"ip-address"`
	IPType    string `json:"ip-address-type"`
}

type CloneRequest struct {
	NewID         int
	Name          string
	TargetNode    string
	Pool          string
	TargetStorage string
	Full          bool
	Snapshot      string
}

type SetConfigRequest struct {
	CloudInitInterface string
	Pool               string
	Tags               []string
	Description        string
	IPConfig           string
	CIUser             string
	SSHKeys            []string
	NameServer         string
	SearchDomain       string
	AgentEnabled       bool
}

func New(cfg Config) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse api_url: %w", err)
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.TLSCAFile != "" {
		pemData, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read tls_ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("load tls_ca_file: no certificates found")
		}
		tlsConfig.RootCAs = pool
	}

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
		authHeader: fmt.Sprintf("PVEAPIToken=%s=%s", cfg.TokenID, cfg.TokenSecret),
	}, nil
}

func (c *Client) GetVersion(ctx context.Context) (Version, error) {
	var version Version
	err := c.get(ctx, "/version", &version)
	return version, err
}

func (c *Client) GetPool(ctx context.Context, pool string) (PoolInfo, error) {
	var out PoolInfo
	err := c.get(ctx, path.Join("/pools", pool), &out)
	return out, err
}

func (c *Client) ListClusterResources(ctx context.Context, resourceType string) ([]ClusterResource, error) {
	query := url.Values{}
	if resourceType != "" {
		query.Set("type", resourceType)
	}
	var resources []ClusterResource
	err := c.getWithQuery(ctx, "/cluster/resources", query, &resources)
	return resources, err
}

func (c *Client) GetNodeStatus(ctx context.Context, node string) (NodeStatus, error) {
	var out NodeStatus
	err := c.get(ctx, path.Join("/nodes", node, "status"), &out)
	return out, err
}

func (c *Client) GetVMConfig(ctx context.Context, node string, vmid int) (VMConfig, error) {
	var out VMConfig
	err := c.get(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "config"), &out)
	return out, err
}

func (c *Client) GetVMStatus(ctx context.Context, node string, vmid int) (VMStatus, error) {
	var out VMStatus
	err := c.get(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "current"), &out)
	return out, err
}

func (c *Client) CloneVM(ctx context.Context, sourceNode string, templateVMID int, req CloneRequest) (string, error) {
	form := url.Values{}
	form.Set("newid", fmt.Sprintf("%d", req.NewID))
	form.Set("name", req.Name)
	form.Set("target", req.TargetNode)
	form.Set("pool", req.Pool)
	if req.TargetStorage != "" {
		form.Set("storage", req.TargetStorage)
	}
	if req.Full {
		form.Set("full", "1")
	} else {
		form.Set("full", "0")
	}
	if req.Snapshot != "" {
		form.Set("snapname", req.Snapshot)
	}
	return c.postString(ctx, path.Join("/nodes", sourceNode, "qemu", fmt.Sprintf("%d", templateVMID), "clone"), form)
}

func (c *Client) SetVMConfig(ctx context.Context, node string, vmid int, req SetConfigRequest) (string, error) {
	form := url.Values{}
	form.Set("pool", req.Pool)
	form.Set("tags", strings.Join(req.Tags, ";"))
	form.Set("description", req.Description)
	if req.CloudInitInterface == "" {
		req.CloudInitInterface = "ipconfig0"
	}
	form.Set(req.CloudInitInterface, req.IPConfig)
	if req.CIUser != "" {
		form.Set("ciuser", req.CIUser)
	}
	if len(req.SSHKeys) > 0 {
		form.Set("sshkeys", strings.Join(req.SSHKeys, "\n"))
	}
	if req.NameServer != "" {
		form.Set("nameserver", req.NameServer)
	}
	if req.SearchDomain != "" {
		form.Set("searchdomain", req.SearchDomain)
	}
	if req.AgentEnabled {
		form.Set("agent", "1")
	}
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "config"), form)
}

func (c *Client) StartVM(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "start"), nil)
}

func (c *Client) ShutdownVM(ctx context.Context, node string, vmid int, timeout time.Duration) (string, error) {
	form := url.Values{}
	if timeout > 0 {
		form.Set("timeout", fmt.Sprintf("%d", int(timeout.Seconds())))
	}
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "shutdown"), form)
}

func (c *Client) StopVM(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "stop"), nil)
}

func (c *Client) DeleteVM(ctx context.Context, node string, vmid int) (string, error) {
	form := url.Values{}
	form.Set("purge", "1")
	return c.deleteString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid)), form)
}

func (c *Client) GetTaskStatus(ctx context.Context, node, upid string) (TaskStatus, error) {
	var out TaskStatus
	err := c.get(ctx, path.Join("/nodes", node, "tasks", upid, "status"), &out)
	return out, err
}

func (c *Client) WaitForTask(ctx context.Context, node, upid string, pollInterval time.Duration) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		status, err := c.GetTaskStatus(ctx, node, upid)
		if err != nil {
			return err
		}
		if status.Status == "stopped" {
			if status.ExitStatus != "" && status.ExitStatus != "OK" {
				return fmt.Errorf("task %s failed: %s", upid, status.ExitStatus)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Client) GetAgentInterfaces(ctx context.Context, node string, vmid int) ([]AgentInterface, error) {
	var out struct {
		Result []AgentInterface `json:"result"`
	}
	err := c.get(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "agent", "network-get-interfaces"), &out)
	return out.Result, err
}

func (c *Client) get(ctx context.Context, p string, out any) error {
	return c.getWithQuery(ctx, p, nil, out)
}

func (c *Client) getWithQuery(ctx context.Context, p string, query url.Values, out any) error {
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, "/api2/json", p)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.authHeader)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return decodeResponse(resp, out)
}

func (c *Client) postString(ctx context.Context, p string, form url.Values) (string, error) {
	return c.writeString(ctx, http.MethodPost, p, form)
}

func (c *Client) deleteString(ctx context.Context, p string, form url.Values) (string, error) {
	return c.writeString(ctx, http.MethodDelete, p, form)
}

func (c *Client) writeString(ctx context.Context, method, p string, form url.Values) (string, error) {
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, "/api2/json", p)

	body := bytes.NewBufferString("")
	if form != nil {
		body = bytes.NewBufferString(form.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", c.authHeader)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out string
	if err := decodeResponse(resp, &out); err != nil {
		return "", err
	}
	return out, nil
}

func decodeResponse(resp *http.Response, out any) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: %s", ErrNotFound, strings.TrimSpace(string(body)))
		}
		return fmt.Errorf("proxmox api error: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var raw envelope[json.RawMessage]
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw.Data, out); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	return nil
}
