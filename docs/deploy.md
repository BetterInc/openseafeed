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
| archiver     | 8084                             | NATS -> ClickHouse batch insert; HTTP: `/healthz` |
| clickhouse   | 8123                             | columnar history store (HTTP interface) |

Replay a recorded NMEA file into the local ingest:

```sh
cargo run -p openseafeed-worker -- forward \
  --nmea-udp-in testdata/replay.nmea \
  --ingest udp://localhost:10110 --key osf_stn_dev
```

Pull live open-government feeds (opt-in `feeds` profile):

```sh
docker compose --profile feeds up -d          # norway + finland connectors
```

Feed connectors are separate, independently deployable workers (one per
upstream, own feed key, runnable on any VPS/Pi/cluster). Their full story —
systemd, compose, and k8s options plus per-feed notes — is in
[connectors.md](connectors.md).

### How data flows

```
receivers / partner feeds / gov feeds
  -> ingest        (UDP 10110, TCP 10111, WSS /v1/ingest; feed-key auth)
  -> NATS JetStream (ais.raw.*)
  -> pipeline      (decode -> validate -> dedupe; ais.decoded.<geohash>)
  -> fanout        (live WebSocket, bbox/MMSI filters)
  -> snapshotter   (periodic full-fleet snapshot on a volume)
  -> archiver      (batch-insert ais.decoded.> into ClickHouse for history)
control serves accounts/OAuth/API keys alongside.
```

All services read `OSF_NATS_URL` (compose/k8s: `nats://nats:4222`). Common env:
`RUST_LOG`, `OSF_KEYS_MODE` (`dev` locally, `http` in prod), `OSF_CONTROL_URL`,
`OSF_INTERNAL_TOKEN`.

## Container image

One image (`Dockerfile`) holds all service binaries; each deployment picks its
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
- **No public UDP on Kapsule.** The Scaleway LoadBalancer does not support UDP,
  so `ingest-udp` gets no external IP on Kapsule — this is expected. Anonymous
  UDP ingest is a dev/LAN convenience only; keep it off in prod
  (`OSF_ALLOW_ANON_UDP=0`). Remote feeds use authenticated TCP/WSS instead.
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
| `OSF_CLICKHOUSE_PASSWORD`  | clickhouse, archiver | password for the ClickHouse `osf` user |

```sh
cp deploy/k8s/base/secret.example.yaml deploy/k8s/base/secret.yaml
# fill in real values (secret.yaml is gitignored)
kubectl -n openseafeed apply -f deploy/k8s/base/secret.yaml
```

## History storage

Long-term AIS history lives in **ClickHouse**, fed by the `archiver` service
(consumes `ais.decoded.>` from NATS, batch-inserts over the HTTP interface on
:8123). The archiver applies its own schema migrations at startup, so the
deploy only has to provide a running ClickHouse with a database (`osf`) and
credentials.

### Why ClickHouse (not TimescaleDB)

- **Columnar + heavy compression.** AIS is a firehose of narrow, repetitive
  rows; ClickHouse gets 10–20x compression, which is what makes a year of
  history affordable.
- **Native tiered storage.** Parts age from the local (hot) disk to a Wasabi
  (S3-compatible) bucket via `TTL ... TO VOLUME 'cold'`, and the *same* table
  stays queryable across both tiers — cold reads are just slower.
- **Lifecycle.** Hot on local disk for 14 days, then moved to Wasabi cold
  (still queryable), then `TTL ... DELETE` at 2 years. These TTLs are part of
  the archiver's schema, not the deploy. Two years rather than one because
  Wasabi bills a 1 TB minimum: at ~0.4 TB/year, the second year is free.

Object storage is on **Wasabi**, a separate S3-compatible provider (compute is
on Scaleway; storage is not). Two Wasabi specifics matter here:

- **No egress fees.** ClickHouse re-reads cold parts every time a historical
  query touches aged data, so a provider that charges per-GB egress would make
  history queries expensive. Wasabi's flat **~$7/TB/month** (1 TB minimum)
  covers a year of AIS history with no egress surprises.
- **90-day minimum storage billing is a non-issue.** Wasabi bills any object
  for at least 90 days even if deleted sooner; our cold parts live ~350 days
  (moved at 14 days, deleted at the 1-year TTL) — well past the minimum — so we
  never pay for storage we didn't use.

### Tiers

```
insert -> hot volume (local PVC, 14 days)
       -> cold volume (Wasabi bucket, queryable, slower)  [TTL ... TO VOLUME 'cold' @ 14d]
       -> deleted                                         [TTL ... DELETE @ 2y]
```

TTLs are baked into the table when the archiver first creates it —
**changing `OSF_HOT_DAYS`/`OSF_RETAIN_DAYS` later does not alter an existing
table**. On a stack that already has data, apply once by hand:

```sql
ALTER TABLE osf.positions MODIFY TTL
  toDateTime(ts) + INTERVAL 14 DAY TO VOLUME 'cold',
  toDateTime(ts) + INTERVAL 730 DAY DELETE
```

(Drop the `TO VOLUME` clause if the Wasabi tier isn't enabled.) When volume
grows 10x with the RF-station wave, prefer **downsampling over shorter
retention**: keep raw ~90 days, roll older data up to one point per minute
per vessel via a materialized view. Not built yet — planned when needed.

If deep-history queries become frequent, add a **local read cache** to the S3
disk (ClickHouse `cache` disk type layered over the Wasabi disk): recently
fetched cold parts are then served from the hot NVMe on repeat queries. One
config block in the same storage ConfigMap; not enabled by default.

### Rough sizing

At ~1k msg/s the network produces roughly:

- 1,000 msg/s × 86,400 s ≈ **86M rows/day**
- After ClickHouse compression, ≈ **~2 GB/day** on the hot disk
- 14-day hot window ≈ **~28 GB** local (the 100Gi PVC leaves generous headroom)
- Cold tier grows ~2 GB/day in Wasabi; a year ≈ **~730 GB**, comfortably inside
  Wasabi's 1 TB minimum at ~$7/month — small next to the compute.

### Enabling the Wasabi cold tier

Off by default (local-only). To turn it on:

1. Create the bucket in the **Wasabi console** (it is intentionally not managed
   by OpenTofu — we don't hand tofu Wasabi root keys). `tofu output
   cold_bucket_endpoint` prints the endpoint/bucket path for reference; adjust
   `cold_bucket_endpoint`/`cold_bucket_name` vars for your region if needed.
2. Fill the placeholders in the `clickhouse-storage` ConfigMap
   (`deploy/k8s/base/clickhouse.yaml`): `<WASABI_ENDPOINT>`,
   `<WASABI_ACCESS_KEY>`, `<WASABI_SECRET_KEY>` (Wasabi keys go in
   `openseafeed-secrets` as `OSF_WASABI_ACCESS_KEY` / `OSF_WASABI_SECRET_KEY`).
   Inject the credentials from your secrets tooling — do not commit them.
3. Uncomment the `storage-config` volume + volumeMount in the ClickHouse
   StatefulSet and re-apply. The `hot_cold` storage policy then becomes
   available for the archiver's `TTL ... TO VOLUME 'cold'` rules.

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
