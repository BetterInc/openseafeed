resource "scaleway_k8s_cluster" "this" {
  name    = var.cluster_name
  version = var.k8s_version
  cni     = "cilium"
  region  = var.region

  # Keep the cluster reachable while pools are updated.
  delete_additional_resources = true

  autoscaler_config {
    balance_similar_node_groups   = true
    scale_down_unneeded_time      = "5m"
    estimator                     = "binpacking"
    expander                      = "random"
    ignore_daemonsets_utilization = true
  }
}

resource "scaleway_k8s_pool" "default" {
  cluster_id = scaleway_k8s_cluster.this.id
  name       = "default"
  node_type  = var.node_type
  size       = var.node_count
  zone       = var.zone

  autoscaling = true
  autohealing = true
  min_size    = var.min_nodes
  max_size    = var.max_nodes

  # Roll nodes cleanly on upgrades.
  upgrade_policy {
    max_surge       = 1
    max_unavailable = 0
  }
}
