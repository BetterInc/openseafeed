# Cold-tier object storage for ClickHouse (parts aged out of the local hot disk
# via TTL ... TO VOLUME 'cold').
#
# Object storage lives on WASABI, a separate S3-compatible provider — compute
# is on Scaleway, storage is not. We deliberately do NOT manage the Wasabi
# bucket with OpenTofu: that would mean handing tofu your Wasabi root keys.
# Create the bucket in the Wasabi console instead, then wire its endpoint +
# access keys into the clickhouse-storage ConfigMap
# (see deploy/k8s/base/clickhouse.yaml).
#
# The cold_bucket_endpoint / cold_bucket_name variables (variables.tf) exist
# only so the endpoint is surfaced as an output for reference — they create no
# resources.
