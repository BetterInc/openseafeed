# OpenSeaFeed

**An open, community-owned AIS network.** Contributors run receivers that feed a shared
aggregation platform; the platform re-broadcasts a free public vessel feed that nobody
can paywall, close, or pull offline.

- 100% open source (Apache-2.0), data published under CC-BY-4.0
- Written in Rust, deployed with OpenTofu on Kubernetes — reproducible by anyone
- Drop-in [aisstream.io](https://aisstream.io)-compatible WebSocket API
- Plug-and-play receiver worker for RTL-SDR / Airspy / HackRF, with antenna health checks
- Accepts feeds from existing tools (AIS-catcher, rtl-ais, any NMEA-over-UDP) and from
  partner networks pushing whole aggregated streams

## How data flows

```
receivers (SDR workers, AIS-catcher, partner networks, open gov feeds)
    → ingest-gateway (UDP/TCP/WSS, feed-key auth)
    → NATS JetStream (ais.raw.*)
    → pipeline (decode → validate → dedupe)   [ais.decoded.<geohash>]
    → fanout-ws (live WebSocket stream, bbox/MMSI filters)
    → snapshotter (full-fleet snapshot every 60 s, CDN-cached)
    → archiver (ClickHouse history)
```

## API

| Endpoint | Free tier | Contributor tier |
|---|---|---|
| `GET /v1/snapshot` — full fleet | 10-minute freshness | 1-minute freshness |
| `wss /v1/stream` — live, aisstream.io-compatible subscribe | limited area/budget | unlimited |
| `wss /v1/ingest` — push your feed to us | — | feed key required |
| `GET /v1/stations` — public coverage stats | open | open |

Contributing data (running a receiver or pushing a feed) automatically earns
contributor tier. Consuming is free for everyone.

## Repository layout

```
services/ingest       UDP/TCP/WSS ingest gateway
services/pipeline     decode, validate, dedupe, publish by geohash
services/fanout       WebSocket streaming (aisstream.io-compatible)
services/snapshotter  full-fleet snapshot generator + HTTP serving
services/control      accounts, OAuth, API keys, tiers, station registry
worker                receiver worker: rf | forward | connect
crates/ais            ITU-R M.1371 decoder library
crates/nmea           NMEA 0183 / AIVDM parsing, multipart reassembly
crates/feed           message envelopes + NATS subject conventions
crates/geo            geohash encoding + bbox covers for routing
crates/keys           API/station/feed key validation
deploy/tofu           OpenTofu infrastructure
deploy/k8s            Kubernetes manifests (kustomize)
```

## Quick start (local dev)

```sh
make dev      # starts NATS + all services via docker compose
make test
# replay a recorded NMEA file into the local ingest:
cargo run -p openseafeed-worker -- forward --replay testdata/replay.nmea \
    --ingest udp://localhost:10110 --key osf_stn_devdevdev
# or run the end-to-end smoke test:
python3 scripts/e2e.py   # needs: pip install websockets requests
```

## Status

Early development. See `docs/` for the architecture plan and milestones.
