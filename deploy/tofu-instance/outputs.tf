output "public_ip" {
  description = "Static public IPv4 of the box — point the DNS A records at this"
  value       = scaleway_instance_ip.this.address
}

output "ssh" {
  description = "Ready-to-paste SSH command"
  value       = "ssh root@${scaleway_instance_ip.this.address}"
}

output "dns_records" {
  description = "A records to create in Cloudflare (all -> public_ip; leave stream. unproxied so WebSockets are not affected by proxy timeouts)"
  value = {
    "stream.${var.domain}" = scaleway_instance_ip.this.address
    "api.${var.domain}"    = scaleway_instance_ip.this.address
    "ingest.${var.domain}" = scaleway_instance_ip.this.address
  }
}

output "endpoints" {
  description = "Public endpoints once DNS resolves and Caddy has issued certificates"
  value = {
    stream  = "wss://stream.${var.domain}/v1/stream"
    api     = "https://api.${var.domain}"
    history = "https://api.${var.domain}/v1/history/{mmsi}"
    ingest  = "wss://ingest.${var.domain}/v1/ingest"
    tcp     = "tcp://ingest.${var.domain}:10111"
  }
}

output "data_volume_id" {
  description = "Block volume mounted at /var/lib/docker (survives instance re-creation)"
  value       = scaleway_block_volume.data.id
}
