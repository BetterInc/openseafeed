# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# OpenSeaFeed — single image, all service binaries.
#
# Every service is a binary in the same Cargo workspace, so we build the whole
# workspace once and copy all binaries into a slim runtime image. Each
# compose/k8s entry chooses which binary to run via `command`.
#
# cargo-chef is used so the dependency compile is cached as its own layer and
# only re-runs when Cargo.toml / Cargo.lock change, not on every source edit.
# ---------------------------------------------------------------------------

# ---- planner: capture the dependency graph -------------------------------
FROM lukemathwalker/cargo-chef:latest-rust-1.92 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: compile deps (cached), then the workspace ------------------
# Two cache levels: cargo-chef gives layer-level dependency caching (works in
# CI), and BuildKit cache mounts persist cargo's incremental target/ across
# local builds — so a source edit only recompiles the crates it touched
# instead of the whole workspace.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo build --release --workspace \
    && mkdir -p /out \
    && cp target/release/openseafeed-ingest target/release/openseafeed-pipeline \
          target/release/openseafeed-fanout target/release/openseafeed-snapshotter \
          target/release/openseafeed-control target/release/openseafeed-archiver \
          target/release/openseafeed-worker /out/

# ---- runtime: slim image with just the binaries --------------------------
FROM debian:bookworm-slim AS runtime
# ca-certificates for outbound TLS (OAuth, upstream feeds); wget for healthchecks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user. /data subdirs are created here so named
# volumes mounted there inherit osf ownership on first use.
RUN useradd --system --uid 10001 --user-group --home /app osf \
    && mkdir -p /data/control /data/snapshots \
    && chown -R osf:osf /data
WORKDIR /app

COPY --from=builder /out/openseafeed-ingest      /usr/local/bin/
COPY --from=builder /out/openseafeed-pipeline    /usr/local/bin/
COPY --from=builder /out/openseafeed-fanout      /usr/local/bin/
COPY --from=builder /out/openseafeed-snapshotter /usr/local/bin/
COPY --from=builder /out/openseafeed-control     /usr/local/bin/
COPY --from=builder /out/openseafeed-archiver    /usr/local/bin/
COPY --from=builder /out/openseafeed-worker      /usr/local/bin/

USER osf

# No default CMD: each service sets its own `command`
# (e.g. ["openseafeed-ingest"] or ["openseafeed-worker","connect", ...]).
