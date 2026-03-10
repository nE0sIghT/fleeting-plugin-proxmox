# Design

## Goals

`fleeting-plugin-proxmox` provides GitLab Fleeting autoscaling on top of Proxmox VE with the following v1 constraints:

- QEMU only
- Cloud-Init networking with either static IPv4 provisioning or DHCP
- strong workload isolation from non-managed hypervisor workloads
- least-privilege API usage

## Main components

- `provider.go`
  Implements the Fleeting `provider.InstanceGroup` contract.
- `config.go`
  Validates plugin configuration and computes defaults.
- `internal/proxmoxclient`
  Encapsulates Proxmox REST calls and async task polling.
- `internal/instancegroup`
  Owns lifecycle orchestration for managed VMs.
- `internal/ippool`
  Allocates, releases, and reconciles static IPv4 leases when `network_mode = "static"`.
- `internal/state`
  Persists lease state to disk with file locking.
- `internal/scheduler`
  Chooses a safe target node while honoring configured headroom.
- datastore selection
  Chooses the most free datastore from a configured allowlist for the selected node.
- `internal/limiter`
  Caps concurrent clone/start/delete activity.

## Managed VM identity

A VM is considered managed only if all of the following are true:

- it is a QEMU VM
- it belongs to the configured Proxmox pool
- it has every mandatory plugin tag
- its VMID is inside the configured dedicated VMID range

Deletion and cleanup never rely only on VM name or IP address.

## Network workflow

For `network_mode = "static"`:

1. Reserve an IP lease from the configured pool.
2. Clone the template VM onto a selected node.
3. Configure Cloud-Init network data through `ipconfig0`.
4. Configure user, SSH keys, DNS, search domain, tags, description and agent settings.
5. Start the VM.
6. Wait until the guest agent reports the configured IP address.
7. Return connect information for SSH.

For `network_mode = "dhcp"`:

1. Clone the template VM onto a selected node.
2. Configure Cloud-Init networking as `ip=dhcp`.
3. Start the VM.
4. Wait until the guest agent reports a usable IPv4 address.
5. Return connect information for SSH.

If any step fails, the plugin destroys the partially created VM and releases the lease.

## Scheduler behavior

The scheduler only considers nodes from an explicit allowlist. A node is eligible only if, after placing one more VM, it still keeps:

- reserved free memory
- reserved free disk
- reserved free CPU headroom

This minimizes impact on unrelated workloads already running on the same hypervisor.

The resource headroom check uses current free resources reported by Proxmox, not a separate reservation model.

## State model

The plugin persists IP allocations in a local JSON state file protected by a lock file. Startup reconciliation rebuilds occupied leases from currently managed VMs and converts stale leases back into reusable entries after a cooldown period.

## Security posture

- dedicated Proxmox pool
- dedicated VMID range
- mandatory plugin tags
- no mutation of template VM
- no host-level networking or migration control
- no raw QEMU argument passthrough
- no arbitrary Cloud-Init custom snippets in v1
