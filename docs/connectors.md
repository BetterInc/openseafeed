# Feed connectors

A **connector** is a small edge process that pulls AIS from one upstream feed
and pushes it into OpenSeaFeed's public ingest endpoint. Key properties:

- **One process per feed.** Norway, Finland, Denmark, and any future source
  each run as their own connector — independent of each other and of the
  platform.
- **Its own feed key.** Each connector authenticates with a feed key tied to
  your account, so the data it contributes is attributed to you (and earns
  contributor tier).
- **Runs anywhere.** A connector needs only outbound network. Run it on a
  spare VPS, a Raspberry Pi at home, our cluster, or someone else's cluster —
  it does not have to live next to the platform.

It is the same binary everywhere: `openseafeed-worker connect`.

```
upstream feed  ──►  openseafeed-worker connect  ──►  ingest.openseafeed.org
(Kystverket,        (--upstream <feed>              (WSS /v1/ingest, or TCP,
 Digitraffic, …)     --key osf_feed_…)               feed-key authenticated)
```

## Getting a feed key

Sign in to the control-plane dashboard (`https://api.openseafeed.org`, GitHub
or Google login) and create a feed key under your account. It looks like
`osf_feed_…`. One key can be reused across your connectors, or use one per feed
so you can revoke them independently.

## Ingest endpoints

The connector's `--ingest` target accepts three URL schemes:

| Scheme | Use | Auth |
|--------|-----|------|
| `wss://host/v1/ingest?key=…` | **recommended over the internet** | TLS + feed key |
| `tcp://host:10111` | authenticated TCP (sends `AUTH <key>` first) | feed key |
| `udp://host:10110` | LAN / dev only | none |

Public endpoint: `wss://ingest.openseafeed.org/v1/ingest`.

## Run options

### 1. Bare VPS / Raspberry Pi (systemd) — recommended for edge nodes

Files are in `deploy/systemd/`. The unit is templated per upstream (`%i`):

```sh
# from a checkout with the release binary built (cargo build --release -p openseafeed-worker):
sudo deploy/systemd/install.sh norway
# edit the feed key:
sudo nano /etc/openseafeed/norway.env      # set OSF_FEED_KEY
sudo systemctl enable --now openseafeed-connector@norway
journalctl -u openseafeed-connector@norway -f
```

Run more feeds by repeating with a different instance name
(`openseafeed-connector@finland`, etc.). Each reads its own
`/etc/openseafeed/<feed>.env`. The unit is hardened (`DynamicUser`,
`ProtectSystem=strict`, `NoNewPrivileges`, restricted syscalls/address
families) — a connector only needs outbound network.

### 2. Docker Compose

Connectors live behind the opt-in `feeds` profile in the root
`docker-compose.yml`:

```sh
docker compose --profile feeds up -d          # norway + finland
# denmark is a commented example — needs OSF_DENMARK_ADDR
```

Set feed keys via `OSF_FEED_KEY_NORWAY` / `OSF_FEED_KEY_FINLAND` (env or
`.env`). In compose these target the local `ingest` service; point them at the
public endpoint by editing `--ingest` if running detached from the stack.

### 3. Kubernetes

`deploy/k8s/base` includes `worker-norway` and `worker-finland` Deployments
(one replica each, tiny resources, feed key from the `openseafeed-secrets`
Secret), pushing to the in-cluster `ingest` ClusterIP. `worker-denmark` is
provided as `worker-denmark.example.yaml` and is **not** wired into
`kustomization.yaml` — enable it once you have a DMA grant (see the file
header). `worker-aisstream.example.yaml` is likewise not wired in (needs
`OSF_AISSTREAM_KEY`). Feed-key secret keys: `OSF_FEED_KEY_NORWAY`,
`OSF_FEED_KEY_FINLAND`, `OSF_FEED_KEY_DENMARK`, `OSF_FEED_KEY_AISSTREAM`.

## Per-feed notes

| Feed | `--upstream` | Access | Extra config |
|------|--------------|--------|--------------|
| Norway     | `norway`     | public (Kystverket TCP)             | none |
| Finland    | `finland`    | public (Digitraffic MQTT/WSS)       | none |
| Denmark    | `denmark`    | requires a per-user grant from DMA  | `OSF_DENMARK_ADDR=tcp://host:port` |
| aisstream.io | `aisstream` | third-party aggregator (needs their API key) | `OSF_AISSTREAM_KEY=<your aisstream.io key>` |

The Denmark connector exits with code 2 and printed instructions if
`OSF_DENMARK_ADDR` is unset.

Consuming a third-party aggregator's feed (e.g. aisstream.io) is subject to
that provider's terms — the operator enabling the connector is responsible for
having the right to contribute that data; its source is tagged
`connect:aisstream` so it can be filtered downstream.
