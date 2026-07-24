# Deploying OpenSeaFeed

OpenSeaFeed is a Rust Cargo workspace of small services glued together by NATS
JetStream. Everything here is reproducible: a laptop runs the whole stack via
docker compose, and production is a Scaleway Kapsule cluster provisioned with
OpenTofu and deployed with plain kustomize manifests.

## Local development

```sh
make dev        # build the image + start NATS and all services
make dev-logs   # follow logs
make dev-down   # tear down (named volumes are kept)
```

`make dev` publishes ports to localhost only:

| Service      | Ports (localhost)                | Purpose |
|--------------|----------------------------------|---------|
| nats         | 4222, 8222                       | message bus + monitoring |
| ingest       | 10110/udp, 10111/tcp, 8080       | feed intake; HTTP: WS `/v1/ingest`, `/healthz`, `/metrics` |
| pipeline     | —                                | decode / validate / dedupe (NATS only) |
| fanout       | 8081                             | live WS `/v1/stream`, `/healthz` |
| snapshotter  | 8082                             | `/v1/snapshot`, `/v1/vessels/{mmsi}`, `/healthz` |
| control      | 8083                             | accounts, OAuth, API keys, `/healthz` |

Replay a recorded NMEA file into the local ingest:

```sh
cargo run -p openseafeed-worker -- forward \
  --nmea-udp-in testdata/replay.nmea \
  --ingest udp://localhost:10110 --key osf_stn_dev
```

Pull a live open-government feed (opt-in `feeds` profile):

```sh
docker compose --profile feeds up -d norway-feed
```

### How data flows

```
receivers / partner feeds / gov feeds
  -> ingest        (UDP 10110, TCP 10111, WSS /v1/ingest; feed-key auth)
  -> NATS JetStream (ais.raw.*)
  -> pipeline      (decode -> validate -> dedupe; ais.decoded.<geohash>)
  -> fanout        (live WebSocket, bbox/MMSI filters)
  -> snapshotter   (periodic full-fleet snapshot on a volume)
control serves accounts/OAuth/API keys alongside.
```

All services read `OSF_NATS_URL` (compose/k8s: `nats://nats:4222`). Common env:
`RUST_LOG`, `OSF_KEYS_MODE` (`dev` locally, `http` in prod), `OSF_CONTROL_URL`,
`OSF_INTERNAL_TOKEN`.

## Container image

One image (`Dockerfile`) holds all six binaries; each deployment picks its
binary via `command`. It is multi-stage (cargo-chef for dependency caching,
`debian:bookworm-slim` runtime) and CI publishes it to
`ghcr.io/openseafeed/openseafeed` (`:latest` and `:<git-sha>` on `main`,
multi-arch amd64+arm64).

## Kubernetes

Manifests live in `deploy/k8s`, laid out for kustomize:

- `base/` — namespace, NATS StatefulSet (3 replicas, JetStream file store),
  Deployments for every service, Services, fanout HPA, PodDisruptionBudgets.
- `overlays/prod/` — nginx Ingress (`stream.` -> fanout, `api.` ->
  snapshotter + control) and image pinning.

```sh
kubectl apply -k deploy/k8s/base            # or deploy/k8s/overlays/prod
```

Notes:

- **Ingest LBs.** Mixed-protocol LoadBalancers are not portable, so ingest UDP
  (`ingest-udp`) and TCP (`ingest-tcp`) are two separate LoadBalancer Services;
  the HTTP port is a plain ClusterIP.
- **Stateful single writers.** snapshotter and control each own a PVC and run
  one replica with the `Recreate` strategy. pipeline and fanout scale freely
  (NATS queue group); fanout is the HPA target (70% CPU, 2–10 replicas).

### Secrets

Applied out-of-band from `deploy/k8s/base/secret.example.yaml` into the
`openseafeed-secrets` Secret. Required keys:

| Key | Used by | What it is |
|-----|---------|-----------|
| `OSF_INTERNAL_TOKEN`       | all       | service-to-service auth token |
| `OSF_SESSION_SECRET`       | control   | cookie/session signing secret |
| `OSF_GITHUB_CLIENT_ID`     | control   | GitHub OAuth app client id |
| `OSF_GITHUB_CLIENT_SECRET` | control   | GitHub OAuth app client secret |
| `OSF_GOOGLE_CLIENT_ID`     | control   | Google OAuth client id |
| `OSF_GOOGLE_CLIENT_SECRET` | control   | Google OAuth client secret |
| `OSF_FEED_KEY_NORWAY`      | worker    | feed key the Norway worker presents to ingest |

```sh
cp deploy/k8s/base/secret.example.yaml deploy/k8s/base/secret.yaml
# fill in real values (secret.yaml is gitignored)
kubectl -n openseafeed apply -f deploy/k8s/base/secret.yaml
```

## Cluster provisioning (OpenTofu / Scaleway)

Full bootstrap steps are in `deploy/tofu/README.md`. In short:

```sh
cd deploy/tofu
tofu init && tofu apply                      # cluster + pool (no DNS yet)
tofu output -raw kubeconfig > kubeconfig.yaml
export KUBECONFIG="$PWD/kubeconfig.yaml"
# install nginx ingress + cert-manager, read the LB IP, then:
kubectl apply -k ../k8s/base
kubectl -n openseafeed apply -f ../k8s/base/secret.yaml
tofu apply -var 'ingress_lb_ip=<LB_IP>' -var 'cloudflare_zone_id=<ZONE_ID>'
```

**Cost:** roughly 3x `DEV1-M` ≈ €50–70/month plus Scaleway LoadBalancers (one
each for ingest UDP and TCP, and the ingress controller's LB).
