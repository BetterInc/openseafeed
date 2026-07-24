# DNS is optional (enable_cloudflare) and only created once the ingress
# LoadBalancer IP is known (ingress_lb_ip). Until then these resources produce
# a count of 0 and are skipped, so the first apply can run before the cluster
# exists.
locals {
  manage_dns = var.enable_cloudflare && var.ingress_lb_ip != ""
}

# api and www are proxied through Cloudflare (caching + WAF for request/reply
# traffic).
resource "cloudflare_record" "api" {
  count   = local.manage_dns ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = "api"
  type    = "A"
  content = var.ingress_lb_ip
  proxied = true
  ttl     = 1
}

resource "cloudflare_record" "www" {
  count   = local.manage_dns ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = "www"
  type    = "A"
  content = var.ingress_lb_ip
  proxied = true
  ttl     = 1
}

# stream is intentionally NOT proxied. Cloudflare's free plan does support
# WebSockets, but these streams are long-lived and latency-sensitive; going
# direct to the LB avoids an extra proxy hop and connection-duration limits.
resource "cloudflare_record" "stream" {
  count   = local.manage_dns ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = "stream"
  type    = "A"
  content = var.ingress_lb_ip
  proxied = false
  ttl     = 300
}
