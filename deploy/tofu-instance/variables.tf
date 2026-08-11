variable "server_name" {
  description = "Name of the instance (also its hostname)"
  type        = string
  default     = "openseafeed"
}

variable "zone" {
  description = "Scaleway zone for the instance, its IP and its data volume"
  type        = string
  default     = "fr-par-1"
}

variable "instance_type" {
  description = <<-EOT
    Scaleway instance type. PRO2-S (2 vCPU / 16 GB) runs the whole stack
    including ClickHouse with headroom; PRO2-XS (2/8) works for a quieter
    network. Block-storage-only types are expected (see root_volume below).
  EOT
  type        = string
  default     = "PRO2-S"
}

variable "volume_size_gb" {
  description = <<-EOT
    Size of the extra block volume mounted at /var/lib/docker. It holds the
    image build cache plus every named volume (ClickHouse hot tier, NATS
    JetStream, snapshots, the control-plane sqlite db). 100 GB fits a ~14-day
    ClickHouse hot window at ~2 GB/day with room to spare.
  EOT
  type        = number
  default     = 100
}

variable "root_volume_size_gb" {
  description = "Size of the OS root volume (only the base system lives here)"
  type        = number
  default     = 40
}

variable "ssh_key_name" {
  description = <<-EOT
    Name of an existing Scaleway SSH key (Console -> Project -> SSH keys) whose
    public key is installed on the box. Scaleway also injects every project key
    automatically; naming one here makes the dependency explicit and fails the
    apply early if it is missing.
  EOT
  type        = string
}

variable "admin_cidr" {
  description = <<-EOT
    CIDR allowed to reach SSH (22/tcp). Use your own address, e.g.
    "203.0.113.7/32" — "0.0.0.0/0" exposes SSH to the whole internet.
  EOT
  type        = string

  validation {
    condition     = can(cidrnetmask(var.admin_cidr))
    error_message = "admin_cidr must be a valid IPv4 CIDR, e.g. 203.0.113.7/32."
  }
}

variable "domain" {
  description = <<-EOT
    Apex domain served by this box (e.g. openseafeed.com). Used for the login
    banner and the endpoint outputs; the A records themselves are created by
    hand in Cloudflare (see docs/production.md).
  EOT
  type        = string
  default     = "openseafeed.com"
}
