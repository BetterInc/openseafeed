variable "cluster_name" {
  description = "Name of the Kapsule cluster"
  type        = string
  default     = "openseafeed"
}

variable "node_type" {
  description = "Scaleway instance type for the worker pool"
  type        = string
  default     = "DEV1-M"
}

variable "node_count" {
  description = "Initial number of nodes in the pool"
  type        = number
  default     = 3
}

variable "min_nodes" {
  description = "Autoscaling floor for the pool"
  type        = number
  default     = 3
}

variable "max_nodes" {
  description = "Autoscaling ceiling for the pool"
  type        = number
  default     = 6
}

variable "region" {
  description = "Scaleway region"
  type        = string
  default     = "fr-par"
}

# Cold-tier object storage lives on Wasabi (created in the Wasabi console, not
# managed here — see storage.tf). These are for reference/outputs only.
variable "cold_bucket_name" {
  description = "Wasabi bucket for the ClickHouse cold tier"
  type        = string
  default     = "openseafeed-ais-cold"
}

variable "cold_bucket_endpoint" {
  description = "Wasabi S3 endpoint for the cold-tier bucket's region"
  type        = string
  default     = "https://s3.eu-central-2.wasabisys.com"
}

variable "zone" {
  description = "Scaleway zone within the region"
  type        = string
  default     = "fr-par-1"
}

variable "k8s_version" {
  description = "Kubernetes version for the cluster"
  type        = string
  default     = "1.30"
}

variable "zone_domain" {
  description = "Apex domain served by the platform (e.g. openseafeed.org)"
  type        = string
  default     = "openseafeed.org"
}

variable "enable_cloudflare" {
  description = "Manage DNS records in Cloudflare for the platform hostnames"
  type        = bool
  default     = true
}

variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID for zone_domain (required when enable_cloudflare)"
  type        = string
  default     = ""
}

variable "ingress_lb_ip" {
  description = <<-EOT
    Public IP of the nginx ingress LoadBalancer. Leave empty on the first apply
    (the ingress controller is installed into the cluster afterwards); set it
    once the LB IP is known and re-apply to create the stream/api/www records.
  EOT
  type        = string
  default     = ""
}
