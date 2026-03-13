package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.com/gitlab-org/fleeting/fleeting/plugin"
	proxmox "gitlab.com/gitlab-org/fleeting/plugins/proxmox"
)

func main() {
	impl := &proxmox.InstanceGroup{}
	installSignalHandler(impl)
	plugin.Main(impl, proxmox.Version)
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
