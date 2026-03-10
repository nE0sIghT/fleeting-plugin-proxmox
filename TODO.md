# TODO

## Scope locked for v1

- QEMU only. LXC is explicitly out of scope.
- Linux guests only.
- SSH only.
- Cloud-Init networking with `static` or `dhcp` modes.
- Cloud-Init and QEMU guest agent are required for the recommended deployment model.
- Support both Fleeting-backed Runner modes in documentation:
  - `instance`
  - `docker-autoscaler`

## Architecture

- Keep the public provider surface in `provider.go` and delegate lifecycle orchestration to `internal/instancegroup`.
- Use a small typed REST client in `internal/proxmoxclient` instead of a large third-party abstraction.
- Store IP allocation state in a persistent backend, starting with a local file backend in `internal/state`.
- Keep scheduling and address allocation separate from the provider implementation:
  - `internal/scheduler`
  - `internal/ippool`
  - `internal/limiter`

## Safety and least privilege

- Require a dedicated Proxmox pool for managed VMs.
- Require mandatory plugin tags on every managed VM in addition to pool membership.
- Require a dedicated VMID range for the plugin.
- Fail closed: never delete or mutate VMs that do not match pool + mandatory tags + VMID range.
- Keep resource headroom on nodes to avoid harming foreign workloads.
- Default to linked clones to reduce storage IO impact.
- Support datastore allowlists and choose the most free candidate datastore on the selected node.

## Functional requirements

- Validate template VM, pool, node allowlist, VMID range, state backend, and IP pool during `Init`.
- Reconcile existing managed VMs into the local IP allocator state on startup.
- Allocate static IPs from a configured pool and inject them through `ipconfig0` when `network_mode = "static"`.
- Configure `ip=dhcp` and discover the address from the guest agent when `network_mode = "dhcp"`.
- Roll back clone/start failures and release the allocated IP.
- Wait for guest agent visibility of the configured IP before reporting a VM as ready.
- Support graceful shutdown with forced stop fallback before destroy.

## Tests

- Unit-test the IP allocator.
- Unit-test the scheduler scoring and headroom checks.
- Unit-test provider lifecycle with a mock Proxmox API server.
