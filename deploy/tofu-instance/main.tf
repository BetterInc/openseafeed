# OpenSeaFeed on ONE Scaleway instance.
#
# This is the cheap, boring production: a single box that builds the image
# locally and runs the whole compose stack behind Caddy. It is a sibling of
# deploy/tofu (Kapsule), not a replacement — see docs/production.md for when to
# graduate to the cluster.
#
# What lands on the box:
#   /var/lib/docker   -> the extra block volume (images, build cache, volumes)
#   /opt/openseafeed  -> the repo, rsync'd/cloned by the first deploy
#
# Nothing here writes secrets: .env is created by hand on the box from
# .env.production.example.

# Fails the plan early if the SSH key name is wrong, and pins the exact key we
# install rather than relying on Scaleway's project-wide key injection.
data "scaleway_iam_ssh_key" "admin" {
  name = var.ssh_key_name
}

# Static public IP, kept across instance re-creation so DNS stays valid.
resource "scaleway_instance_ip" "this" {
  zone = var.zone
  type = "routed_ipv4"
  tags = ["openseafeed"]
}

# Data volume for /var/lib/docker. Separate from the root volume so the OS can
# be rebuilt without touching ClickHouse's hot tier or the control-plane db.
# 5000 IOPS is the standard Scaleway block tier.
resource "scaleway_block_volume" "data" {
  name       = "${var.server_name}-data"
  zone       = var.zone
  size_in_gb = var.volume_size_gb
  iops       = 5000
  tags       = ["openseafeed"]
}

# Inbound: HTTP/HTTPS for Caddy, TCP 10111 for remote feed connectors, SSH from
# the admin CIDR only. Everything else is dropped — note there is deliberately
# NO udp/10110: anonymous UDP ingest is a dev/LAN convenience and stays off in
# production (OSF_ALLOW_ANON_UDP=0), remote feeds authenticate over TCP/WSS.
# The group is stateful, so replies to outbound connections are allowed without
# matching inbound rules. ICMP is dropped too, so the box will not answer ping.
resource "scaleway_instance_security_group" "this" {
  name                    = var.server_name
  zone                    = var.zone
  description             = "OpenSeaFeed single-box: web + feed ingest, SSH restricted"
  inbound_default_policy  = "drop"
  outbound_default_policy = "accept"

  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 80
  }

  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 443
  }

  # NMEA over TCP — how off-box feed connectors and receivers push in.
  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 10111
  }

  inbound_rule {
    action   = "accept"
    protocol = "TCP"
    port     = 22
    ip_range = var.admin_cidr
  }
}

resource "scaleway_instance_server" "this" {
  name  = var.server_name
  zone  = var.zone
  type  = var.instance_type
  image = "ubuntu_noble" # Ubuntu 24.04 LTS

  ip_id             = scaleway_instance_ip.this.id
  security_group_id = scaleway_instance_security_group.this.id

  additional_volume_ids = [scaleway_block_volume.data.id]

  # PRO2 and friends have no local storage: the root volume is block storage.
  root_volume {
    volume_type           = "sbs_volume"
    size_in_gb            = var.root_volume_size_gb
    sbs_iops              = 5000
    delete_on_termination = true
  }

  user_data = {
    cloud-init = templatefile("${path.module}/cloud-init.yaml.tftpl", {
      server_name    = var.server_name
      domain         = var.domain
      ssh_public_key = trimspace(data.scaleway_iam_ssh_key.admin.public_key)
    })
  }

  tags = ["openseafeed", "prod"]

  # Resize in place (the provider stops the server, changes the type, starts it)
  # rather than destroying a box that holds the data volume and its state.
  replace_on_type_change = false
}
