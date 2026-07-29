# Upstream feed research (July 2026)

Catalog of AIS sources OpenSeaFeed can import, beyond what already runs.
Per feed: what it is, how to get it, license posture, and connector effort.

## Already integrated

| Feed | Kind | Status |
|------|------|--------|
| aisstream.io | live WS, world (volunteer terrestrial) | running (`connect --upstream aisstream`) |
| Norway — Kystverket | live TCP NMEA, public | running |
| Finland — Digitraffic | live MQTT/WSS JSON, public | running |
| Denmark — DMA daily dumps | historical CSV zips, public | running (`import-dma`) |
| Denmark — DMA live | live TCP, per-user grant | connector ready, needs grant |

## New candidates, ranked

### 1. United States — MarineCadastre.gov (USCG NAIS) · HIGH
Daily, Zstd-compressed CSVs of all US coastal waters since 2015 (1-minute
filtered), plus analysis-ready GeoParquet for 2024/2025. US-government work =
public domain, no license friction. This is the DMA-importer pattern applied
to a coastline far bigger than Denmark: an `import-uscg` worker mode gives us
deep US historical coverage. Live NAIS access exists but requires a USCG
agreement (worth pursuing under the non-profit later).
Links: https://hub.marinecadastre.gov/datasets/vessel-traffic-ais-1 ·
https://marinecadastre.gov/accessais/ ·
https://github.com/ocm-marinecadastre/ais-vessel-traffic

### 2. AISHub — reciprocal live world feed · HIGH (gated on milestone 3)
Contribute ≥1 own receiver (≥10 vessels average coverage, ≥90% uptime over 7
days) → receive the FULL aggregated network as raw NMEA (TCP/UDP) — a direct
aisstream-class upstream with one fewer intermediary. Fits our ingest with a
plain `connect --upstream tcp://…`. Gate: needs our first real RF station
(milestone 3), and their data is for member use — same public-rebroadcast
gray zone as aisstream; tag `connect:aishub` and decide re-serving policy
deliberately. https://www.aishub.net/join-us

### 3. Australia — AMSA Craft Tracking System · MEDIUM
Monthly historical CSVs (terrestrial + satellite!) for the Australian search
and rescue region, free but behind a click-through license agreement, size
caps per download. Importer effort similar to DMA; satellite-sourced points
make it unusually complete offshore. https://www.operations.amsa.gov.au/spatial/

### 4. Global Fishing Watch — enrichment, not feed · ADJACENT
Free APIs (account + attribution required): apparent fishing effort, vessel
identity/registry, events (encounters, loitering, port visits, AIS gaps) and
SAR vessel detections from 2017 to ~5 days ago. Not raw AIS and delayed —
wrong shape for the live feed, ideal for a dark-ship/enrichment layer next to
the archive (the darkships.org angle). https://globalfishingwatch.org/our-apis/

## Dead ends (checked July 2026)

- **Sweden (Sjöfartsverket):** detailed AIS is paid; open APIs cover port
  calls/pilotage only.
- **Estonia / Latvia / other Baltics:** nothing public found; HELCOM offers
  aggregate datasets on request, not a feed.
- **MarineTraffic / VesselFinder / ShipXplorer station programs:** hosts get
  account credits, not raw network feeds — not usable as an upstream.
- **Commercial (Kpler, Spire, VT Explorer, VesselFinder):** paid
  subscriptions, redistribution-restricted — out of scope by mission.

## Recommended order

1. `import-uscg` (MarineCadastre daily files) — biggest coverage win per hour
   of work, zero license risk.
2. AMSA importer when someone wants Oceania history.
3. AISHub reciprocal feed the week our first RF station meets their bar —
   then re-weigh how much we still need aisstream.
4. GFW enrichment layer as its own future service, not a connector.
