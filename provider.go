package proxmox

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path"

	"github.com/hashicorp/go-hclog"
	"golang.org/x/crypto/ssh"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/instancegroup"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/ippool"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/limiter"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/scheduler"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/state"
)

var _ provider.InstanceGroup = (*InstanceGroup)(nil)

type InstanceGroup struct {
	pluginConfig

	log      hclog.Logger
	settings provider.Settings

	client *proxmoxclient.Client
	group  *instancegroup.Group
	sshPub string
}

func (g *InstanceGroup) Init(ctx context.Context, log hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	g.log = log.With("cluster", g.ClusterName, "pool", g.Pool, "template_vmid", g.TemplateVMID)
	g.settings = settings

	if err := g.config().validate(g.settings); err != nil {
		return provider.ProviderInfo{}, err
	}

	if g.settings.Protocol == "" {
		g.settings.Protocol = provider.ProtocolSSH
	}
	if g.settings.Username == "" && g.CIUser != "" {
		g.settings.Username = g.CIUser
	}

	publicKey, err := g.prepareSSHCredentials()
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	g.client, err = proxmoxclient.New(proxmoxclient.Config{
		BaseURL:            g.APIURL,
		TokenID:            g.TokenID,
		TokenSecret:        g.TokenSecret,
		TLSCAFile:          g.TLSCAFile,
		InsecureSkipVerify: g.TLSInsecureSkipVerify,
	})
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	var pool *ippool.Pool
	if g.NetworkMode == "static" {
		store := state.NewFileStore(g.StateFile)
		pool, err = ippool.New(ippool.Config{
			Prefix:        g.parsedPoolPrefix,
			Gateway:       g.parsedGateway,
			Ranges:        g.IPPoolRanges,
			Exclude:       g.IPPoolExclude,
			ReuseCooldown: g.parsedIPReuseCooldown,
		}, store)
		if err != nil {
			return provider.ProviderInfo{}, err
		}
	}

	g.group = instancegroup.New(
		g.client,
		g.log,
		instancegroup.Config{
			ClusterName:           g.ClusterName,
			Pool:                  g.Pool,
			TemplateVMID:          g.TemplateVMID,
			VMIDMin:               g.parsedVMIDRange.Min,
			VMIDMax:               g.parsedVMIDRange.Max,
			NamePrefix:            g.NamePrefix,
			Nodes:                 g.Nodes,
			CloneMode:             g.CloneMode,
			TargetStorages:        g.TargetStorages,
			CloneSnapshot:         g.CloneSnapshot,
			MandatoryTags:         g.mandatoryTags(),
			DescriptionTemplate:   g.DescriptionTemplate,
			CloudInitInterface:    g.CloudInitInterface,
			NetworkMode:           g.NetworkMode,
			CIUser:                g.CIUser,
			NameServers:           g.NameServers,
			SearchDomain:          g.SearchDomain,
			TaskPollInterval:      g.parsedTaskPoll,
			CloneTimeout:          g.parsedCloneTimeout,
			StartTimeout:          g.parsedStartTimeout,
			ShutdownTimeout:       g.parsedShutdownTimeout,
			AgentTimeout:          g.parsedAgentTimeout,
			AgentRequired:         g.AgentRequired,
			GeneratedSSHPublicKey: publicKey,
			StaticSSHPublicKeys:   g.CISSHKeys,
			Scheduler:             scheduler.New(g.Scheduler),
			Reserve: scheduler.Reserve{
				MemoryMB: g.NodeReserveMemoryMB,
				DiskGB:   g.NodeReserveDiskGB,
				CPUCores: g.NodeReserveCPUCores,
			},
		},
		pool,
		limiter.New(g.MaxParallelClones),
		limiter.New(g.MaxParallelStarts),
		limiter.New(g.MaxParallelDeletes),
	)

	if err := g.group.Init(ctx); err != nil {
		return provider.ProviderInfo{}, err
	}

	return provider.ProviderInfo{
		ID:        path.Join("proxmox", g.ClusterName, g.Pool, g.NamePrefix),
		MaxSize:   g.parsedVMIDRange.Max - g.parsedVMIDRange.Min + 1,
		Version:   Version.String(),
		BuildInfo: Version.BuildInfo(),
	}, nil
}

func (g *InstanceGroup) Update(ctx context.Context, update func(instance string, state provider.State)) error {
	instances, err := g.group.List(ctx)
	if err != nil {
		return err
	}
	for _, instance := range instances {
		update(instance.ID, instance.State)
	}
	return nil
}

func (g *InstanceGroup) Increase(ctx context.Context, delta int) (int, error) {
	created, err := g.group.Increase(ctx, delta)
	return len(created), err
}

func (g *InstanceGroup) Decrease(ctx context.Context, instances []string) ([]string, error) {
	return g.group.Decrease(ctx, instances)
}

func (g *InstanceGroup) ConnectInfo(ctx context.Context, instance string) (provider.ConnectInfo, error) {
	info, err := g.group.ConnectInfo(ctx, instance, g.settings)
	if err != nil {
		return provider.ConnectInfo{}, err
	}
	info.Key = g.settings.Key
	return info, nil
}

func (g *InstanceGroup) Heartbeat(ctx context.Context, instance string) error {
	return g.group.Heartbeat(ctx, instance)
}

func (g *InstanceGroup) Shutdown(ctx context.Context) error {
	return nil
}

func (g *InstanceGroup) prepareSSHCredentials() (string, error) {
	if g.settings.UseStaticCredentials && len(g.settings.Key) > 0 {
		pub, err := publicKeyFromPrivate(g.settings.Key)
		if err != nil {
			return "", err
		}
		g.sshPub = pub
		return pub, nil
	}

	if len(g.settings.Key) > 0 {
		pub, err := publicKeyFromPrivate(g.settings.Key)
		if err != nil {
			return "", err
		}
		g.sshPub = pub
		return pub, nil
	}

	pub, priv, err := generateSSHKeyPair()
	if err != nil {
		return "", err
	}
	g.settings.Key = priv
	g.sshPub = pub
	return pub, nil
}

func generateSSHKeyPair() (string, []byte, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, err
	}

	sshPub, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		return "", nil, err
	}

	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return "", nil, err
	}

	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	return string(ssh.MarshalAuthorizedKey(sshPub)), privatePEM, nil
}

func publicKeyFromPrivate(privateKey []byte) (string, error) {
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return "", fmt.Errorf("parse private key: %w", err)
	}
	return string(ssh.MarshalAuthorizedKey(signer.PublicKey())), nil
}
