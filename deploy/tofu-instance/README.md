# OpenSeaFeed on one Scaleway instance (OpenTofu)

The small production: **one** Scaleway instance running the docker compose
stack behind Caddy. Separate root module from `../tofu` (Kapsule) — they share
nothing but the provider, so you can run this today and move to the cluster
later without unpicking state.

Provisioning stops at "a box with Docker and an empty `/opt/openseafeed`". The
first deploy (repo, `.env`, `docker compose up -d --build`) is a human
following [`docs/production.md`](../../docs/production.md) — a fresh box must
never boot into a half-configured stack with no secrets.

## Prerequisites

- [OpenTofu](https://opentofu.org) >= 1.6
- Scaleway API keys exported:
  ```sh
  export SCW_ACCESS_KEY=... SCW_SECRET_KEY=...
  export SCW_DEFAULT_PROJECT_ID=... SCW_DEFAULT_ORGANIZATION_ID=...
  ```
- An SSH key uploaded in the Scaleway console (Project -> SSH keys); pass its
  **name** as `ssh_key_name`.

## Apply

```sh
cp backend.tf.example backend.tf   # optional: remote state
tofu init
tofu apply \
  -var 'ssh_key_name=<your-scaleway-ssh-key-name>' \
  -var 'admin_cidr=<your.ip.addr.ess>/32' \
  -var 'domain=openseafeed.com'
tofu output public_ip              # -> the A records
tofu output -raw ssh               # -> ssh root@<ip>
```

Cloud-init takes ~2-3 minutes after the instance reports ready
(`cloud-init status --wait`, logs in `/var/log/cloud-init-output.log`).

## What gets created

- `scaleway_instance_ip` — static IPv4, kept across instance re-creation so DNS
  stays valid.
- `scaleway_instance_server` — Ubuntu 24.04, `PRO2-S` by default, block-storage
  root volume.
- `scaleway_block_volume` — `volume_size_gb` (default 100 GB) mounted at
  `/var/lib/docker` by cloud-init, so images, the build cache and every named
  volume (ClickHouse hot tier, NATS JetStream, snapshots, control sqlite) live
  on a volume the OS can be rebuilt without touching.
- `scaleway_instance_security_group` — inbound **drop** by default, accepting
  `80/tcp` + `443/tcp` (Caddy) and `10111/tcp` (NMEA over TCP) from anywhere,
  `22/tcp` from `admin_cidr` only. Deliberately **no `10110/udp`**: anonymous
  UDP ingest is a dev/LAN convenience and stays off in production
  (`OSF_ALLOW_ANON_UDP=0`). The group is stateful, so outbound replies need no
  rules; ICMP is dropped, so the box does not answer ping.

## DNS

Not managed here — the single-box deploy points three records at one static IP,
which is faster done in the Cloudflare dashboard than in state. `tofu output
dns_records` prints exactly what to create. Keep `stream.` **unproxied** (grey
cloud): long-lived WebSockets through the Cloudflare proxy hit its idle
timeouts, and Caddy needs to see the real hostname to issue its own certificate.

## Cost

One `PRO2-S` ≈ €22/month plus ~€8/month for 140 GB of block storage and ~€1 for
the static IP — call it **€30/month**, against €50-70 plus load balancers for
the Kapsule path. Wasabi (cold tier + backups) is billed separately at ~$7/TB.
