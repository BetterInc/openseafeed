# OpenSeaFeed infrastructure (OpenTofu, Scaleway Kapsule)

Reproducible cluster provisioning for OpenSeaFeed. Default provider is
[Scaleway Kapsule](https://www.scaleway.com/en/kubernetes-kapsule/); DNS is
managed in Cloudflare (optional).

## Prerequisites

- [OpenTofu](https://opentofu.org) >= 1.6
- A Scaleway account with a project, and API keys exported:
  ```sh
  export SCW_ACCESS_KEY=... SCW_SECRET_KEY=...
  export SCW_DEFAULT_PROJECT_ID=... SCW_DEFAULT_ORGANIZATION_ID=...
  ```
- (optional) Cloudflare API token for DNS: `export CLOUDFLARE_API_TOKEN=...`

## Bootstrap order

1. **Provision the cluster.** DNS is skipped on this pass because the ingress
   LB IP does not exist yet.
   ```sh
   cp backend.tf.example backend.tf   # optional: remote state
   tofu init
   tofu apply
   ```

2. **Get the kubeconfig** and point kubectl at the new cluster.
   ```sh
   tofu output -raw kubeconfig > kubeconfig.yaml
   export KUBECONFIG="$PWD/kubeconfig.yaml"
   kubectl get nodes
   ```

3. **Install an nginx ingress controller and cert-manager** (Helm), then read
   the LoadBalancer IP the ingress controller was assigned:
   ```sh
   kubectl -n ingress-nginx get svc ingress-nginx-controller \
     -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
   ```

4. **Deploy the platform manifests.**
   ```sh
   kubectl apply -k ../k8s/base
   # or the prod overlay (adds Ingress + image pinning):
   kubectl apply -k ../k8s/overlays/prod
   ```

5. **Create the secret** (never committed — see `../k8s/base/secret.example.yaml`):
   ```sh
   cp ../k8s/base/secret.example.yaml ../k8s/base/secret.yaml
   # edit real values, then:
   kubectl -n openseafeed apply -f ../k8s/base/secret.yaml
   ```

6. **Create DNS records.** Feed the ingress LB IP back into tofu and re-apply:
   ```sh
   tofu apply -var 'ingress_lb_ip=<LB_IP>' -var 'cloudflare_zone_id=<ZONE_ID>'
   ```

## What gets created

- `scaleway_k8s_cluster` + one autoscaling pool (`min_nodes`..`max_nodes`,
  default 3..6, `DEV1-M`).
- Cloudflare A records for `api`, `www` (proxied) and `stream` (unproxied,
  see the comment in `dns.tf`) — only when `enable_cloudflare` is true and
  `ingress_lb_ip` is set.

## Cost

Roughly 3x `DEV1-M` ≈ €50–70/month plus the Scaleway LoadBalancer(s). The
ingest UDP and TCP endpoints each need their own single-protocol LB, so budget
for those on top.
