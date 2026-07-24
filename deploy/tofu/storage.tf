# Cold-tier object storage for ClickHouse tiered storage (parts aged out of the
# local hot disk via TTL ... TO VOLUME 'cold'). ClickHouse talks to this over
# the S3-compatible endpoint; wire the endpoint + credentials into the
# clickhouse-storage ConfigMap (see deploy/k8s/base/clickhouse.yaml).
resource "scaleway_object_bucket" "ais_cold" {
  name   = var.cold_bucket_name
  region = var.region

  tags = {
    app  = "openseafeed"
    tier = "cold"
  }
}
