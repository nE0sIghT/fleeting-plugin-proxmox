package proxmox

import "gitlab.com/gitlab-org/fleeting/fleeting/plugin"

var (
	NAME      = "fleeting-plugin-proxmox"
	VERSION   = "dev"
	REVISION  = "HEAD"
	REFERENCE = "HEAD"
	BUILT     = "now"

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
