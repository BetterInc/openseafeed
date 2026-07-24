output "cluster_id" {
  description = "Kapsule cluster ID"
  value       = scaleway_k8s_cluster.this.id
}

output "kubeconfig" {
  description = "Raw kubeconfig for the cluster. Write to a file and export KUBECONFIG."
  value       = scaleway_k8s_cluster.this.kubeconfig[0].config_file
  sensitive   = true
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint"
  value       = scaleway_k8s_cluster.this.apiserver_url
}

output "cold_bucket_name" {
  description = "ClickHouse cold-tier Wasabi bucket name (created in the Wasabi console)"
  value       = var.cold_bucket_name
}

output "cold_bucket_endpoint" {
  description = "Wasabi S3 endpoint + bucket path for the cold tier (use in clickhouse-storage ConfigMap)"
  value       = "${var.cold_bucket_endpoint}/${var.cold_bucket_name}"
}

output "endpoints" {
  description = "Public hostnames served by the platform"
  value = {
    stream = "wss://stream.${var.zone_domain}/v1/stream"
    api    = "https://api.${var.zone_domain}"
    www    = "https://www.${var.zone_domain}"
  }
}
