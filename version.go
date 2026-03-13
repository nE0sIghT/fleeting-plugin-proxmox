package proxmox

import "gitlab.com/gitlab-org/fleeting/fleeting/plugin"

var (
	NAME      = "fleeting-plugin-proxmox"
	VERSION   = "dev"
	REVISION  = "unknown"
	REFERENCE = "unknown"
	BUILT     = "unknown"

	Version plugin.VersionInfo
)

func init() {
	Version = plugin.VersionInfo{
		Name:      NAME,
		Version:   VERSION,
		Revision:  REVISION,
		Reference: REFERENCE,
		BuiltAt:   BUILT,
	}
}
