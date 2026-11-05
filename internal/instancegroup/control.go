package instancegroup

import "gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"

func (g *Group) EnableControl() { g.placement.Enable() }

func (g *Group) ApplyControl(state metrics.DrainState) {
	g.placement.Apply(state, g.cfg.ClusterName)
	g.log.Info("placement control applied", "revision", state.Revision)
}
