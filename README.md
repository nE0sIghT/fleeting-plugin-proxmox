# Fleeting Plugin Proxmox

A GitLab Fleeting plugin for Proxmox VE.


## v1 scope

- QEMU only
- Linux guests only
- SSH only
- Cloud-Init networking with either static IPv4 assignment or DHCP
- dedicated Proxmox pool, dedicated VMID range, mandatory management tags

## Executor modes

The plugin works with both GitLab Runner Fleeting-based executors:

- `instance`
  The job runs directly on the ephemeral VM over the normal instance connector flow.
- `docker-autoscaler`
  The ephemeral VM acts as a Docker host, and jobs run in Docker containers inside that VM.

The plugin itself does not need different provisioning logic for these modes. In both cases it only needs to:

- create the VM
- wait until the VM is reachable
- return connection details to GitLab Runner

The difference is on the Runner side:

- `instance` expects a usable job host OS
- `docker-autoscaler` expects a usable Docker host inside the VM template

## Core configuration

Required plugin configuration:

- `api_url`
- `token_id`
- `token_secret`
- `pool`
- `template_vmid`
- `name_prefix`
- `vmid_range`
- `nodes`
- `network_mode`

Recommended plugin configuration:

- `ip_pool_network` and `ip_pool_gateway` for `network_mode = "static"`
- `ip_pool_ranges` for `network_mode = "static"`
- `target_storages` to constrain clone placement to an allowlist of datastores
- `state_file`
- `ci_user`
- `ci_ssh_keys`
- `nameserver`
- `searchdomain`
- `node_reserve_memory_mb`
- `node_reserve_cpu_cores`
- `node_reserve_disk_gb`

## Plugin configuration reference

- `api_url`  
  Proxmox API base URL, for example `https://pve.example.com:8006`. Required.
- `token_id`  
  Proxmox API token ID. Required.
- `token_secret`  
  Proxmox API token secret. Required.
- `tls_ca_file`  
  Optional CA bundle used to verify the Proxmox TLS certificate.
- `tls_insecure_skip_verify`  
  Disables TLS certificate verification. Intended only for development.
- `cluster_name`  
  Logical cluster identifier used in the Fleeting provider ID. Default: `default`.
- `pool`  
  Dedicated Proxmox pool for managed VMs. Required.
- `template_vmid`  
  Source QEMU template VMID. Required.
- `name_prefix`  
  Prefix used for created VM names and management tags. Required.
- `vmid_range`  
  Dedicated VMID range in `start-end` form. Required.
- `nodes`  
  Node allowlist used for placement. Required. Accepts a string or list.
- `clone_mode`  
  `linked` or `full`. Default: `linked`.
- `target_storages`  
  Optional datastore allowlist. Accepts a string or list. The plugin chooses the most free matching datastore for the selected node.
- `clone_snapshot`  
  Optional snapshot name to use when cloning from the template.
- `node_reserve_memory_mb`  
  Minimum free memory that must remain on the selected node after placement.
- `node_reserve_cpu_cores`  
  Minimum free CPU headroom that must remain on the selected node after placement.
- `node_reserve_disk_gb`  
  Minimum free disk that must remain on the selected node after placement.
- `scheduler`  
  Node selection policy: `balanced`, `most_free_ram`, `most_free_cpu`, or `round_robin`. Default: `balanced`.
- `max_parallel_clones`  
  Maximum concurrent clone operations. Default: `2`.
- `max_parallel_starts`  
  Maximum concurrent start operations. Default: `4`.
- `max_parallel_deletes`  
  Maximum concurrent delete operations. Default: `2`.
- `task_poll_interval`  
  Poll interval for Proxmox async task completion. Default: `2s`.
- `clone_timeout`  
  Timeout for clone operations. Default: `10m`.
- `start_timeout`  
  Timeout for VM start and readiness wait. Default: `5m`.
- `shutdown_timeout`  
  Timeout for graceful shutdown before forced stop. Default: `2m`.
- `cloud_init_enabled`  
  Must remain `true` in v1. Default: `true`.
- `cloud_init_interface`  
  Must be `ipconfig0` in v1. Default: `ipconfig0`.
- `network_mode`  
  `static` or `dhcp`. Default: `static`.
- `ci_user`  
  Optional Cloud-Init login user. Also becomes the default SSH username returned by the plugin.
- `ci_ssh_keys`  
  Optional Cloud-Init public SSH keys. Accepts a string or list.
- `nameserver`  
  Optional DNS servers passed to Cloud-Init. Accepts a string or list.
- `searchdomain`  
  Optional DNS search domain passed to Cloud-Init.
- `ip_pool_network`  
  IPv4 subnet used for static allocation. Required only for `network_mode = "static"`.
- `ip_pool_gateway`  
  IPv4 gateway used for static allocation. Required only for `network_mode = "static"`.
- `ip_pool_ranges`  
  Optional address ranges within `ip_pool_network` used for static allocation. Accepts a string or list.
- `ip_pool_exclude`  
  Optional addresses excluded from static allocation. Accepts a string or list.
- `ip_pool_reuse_cooldown`  
  Cooldown before a released static IP can be reused. Default: `10m`.
- `state_file`  
  State file used by the static IP allocator. Default: `${TMPDIR}/fleeting-plugin-proxmox/state.json`.
- `agent_required`  
  Whether QEMU guest agent is required for readiness and IP discovery. Effectively required for recommended deployments and mandatory for `network_mode = "dhcp"`. Default: `true`.
- `agent_timeout`  
  Timeout for guest agent IP discovery. Default: `3m`.
- `prefer_ipv6`  
  Unsupported in v1 and must not be enabled.
- `tags`  
  Optional extra management tags added to created VMs. Accepts a string or list.
- `description_template`  
  Optional Go text/template for VM descriptions. Template variables: `.Node`, `.VMID`, `.IP`, `.Pool`.

## Safety model

The plugin only manages VMs that satisfy all of the following:

- QEMU VM
- member of the configured Proxmox pool
- within the configured VMID range
- carrying the mandatory plugin tags

This prevents accidental cleanup of unrelated workloads on the same hypervisor.

## Static IP provisioning

The plugin allocates an IP from a configured pool, writes it through Cloud-Init, persists the lease in a local state file, and reconciles leases against currently managed VMs on startup.

When `network_mode = "dhcp"`, the plugin configures `ip=dhcp` in Cloud-Init and discovers the actual address through the QEMU guest agent before returning `ConnectInfo`.

## Datastore selection

Datastore placement works as follows:

- `target_storages`
  Preferred option. Accepts a single string or a list. The plugin chooses the datastore with the most currently free space among the configured candidates that are usable from the selected node.
- neither set
  The plugin leaves datastore selection to Proxmox or template defaults.

## Node reserve semantics

`node_reserve_memory_mb`, `node_reserve_cpu_cores`, and `node_reserve_disk_gb` are admission filters based on the current free resources reported by Proxmox for a node.

They are evaluated as:

- current free memory minus the template VM memory must stay above `node_reserve_memory_mb`
- current free CPU headroom minus the template VM vCPU count must stay above `node_reserve_cpu_cores`
- current free disk minus the template VM disk size must stay above `node_reserve_disk_gb`

They do not create reservations in Proxmox and they do not use a separate reservation accounting model.

## Example GitLab Runner configuration

See [`examples/config.toml`](/workspace/fleeting-plugin-proxmox/examples/config.toml) for a complete example.

Minimal shape:

```toml
[[runners]]
  name = "proxmox-fleeting"
  url = "https://gitlab.example.com"
  token = "RUNNER_TOKEN"
  executor = "instance"

  [runners.autoscaler]
    plugin = "/path/to/fleeting-plugin-proxmox"
    capacity_per_instance = 1
    max_use_count = 1
    max_instances = 20
    instance_ready_command = "cloud-init status --wait || test $? -eq 2"

  [runners.autoscaler.plugin_config]
    api_url = "https://pve.example.com:8006"
    token_id = "gitlab-runner@pve!fleeting"
    token_secret = "REDACTED"
    cluster_name = "prod-pve"
    pool = "gitlab-ci"
    template_vmid = 9000
    name_prefix = "glr"
    vmid_range = "500000-500999"
    nodes = ["pve01", "pve02"]
    target_storages = ["ceph-vm", "local-lvm"]
    network_mode = "static"
    ip_pool_network = "10.10.20.0/24"
    ip_pool_gateway = "10.10.20.1"
    ip_pool_ranges = ["10.10.20.100-10.10.20.199"]
    state_file = "/var/lib/fleeting-plugin-proxmox/state.json"
    ci_user = "ubuntu"
    ci_ssh_keys = ["ssh-ed25519 AAAA... runner@example"]

  [runners.autoscaler.connector_config]
    username = "ubuntu"
    use_external_addr = false
    protocol = "ssh"
```

See [`examples/docker-autoscaler.config.toml`](/workspace/fleeting-plugin-proxmox/examples/docker-autoscaler.config.toml) for a `docker-autoscaler` example.

## Notes

- The template VM should already have Cloud-Init and QEMU guest agent enabled.
- Use a dedicated Proxmox pool and a dedicated VMID range.
- Use a subnet dedicated to ephemeral runner VMs. Do not share it with manually managed VMs.
- `network_mode = "dhcp"` skips the local IP allocator and requires the guest agent to report the acquired address.
- For `docker-autoscaler`, the template VM must already contain a working Docker Engine configuration suitable for GitLab Runner.

## GitHub Actions

The repository includes GitHub Actions workflows:

- [`ci.yml`](/workspace/fleeting-plugin-proxmox/.github/workflows/ci.yml): formatting, module hygiene, tests, and build on pushes and pull requests.
- [`release.yml`](/workspace/fleeting-plugin-proxmox/.github/workflows/release.yml): builds release binaries for tagged versions and uploads `.tar.gz` artifacts to the GitHub Release.
