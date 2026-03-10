package main

import (
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin"
	proxmox "gitlab.com/gitlab-org/fleeting/plugins/proxmox"
)

func main() {
	plugin.Main(&proxmox.InstanceGroup{}, proxmox.Version)
}
