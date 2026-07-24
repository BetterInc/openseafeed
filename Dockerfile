# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# OpenSeaFeed — single image, all six service binaries.
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
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build & cache dependencies only. This layer is reused until Cargo deps change.
RUN cargo chef cook --release --recipe-path recipe.json
# Now copy the full source and build the actual binaries.
COPY . .
RUN cargo build --release --workspace

# ---- runtime: slim image with just the binaries --------------------------
FROM debian:bookworm-slim AS runtime
# ca-certificates for outbound TLS (OAuth, upstream feeds); wget for healthchecks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user.
RUN useradd --system --uid 10001 --user-group --home /app osf
WORKDIR /app

COPY --from=builder /app/target/release/openseafeed-ingest      /usr/local/bin/
COPY --from=builder /app/target/release/openseafeed-pipeline    /usr/local/bin/
COPY --from=builder /app/target/release/openseafeed-fanout      /usr/local/bin/
COPY --from=builder /app/target/release/openseafeed-snapshotter /usr/local/bin/
COPY --from=builder /app/target/release/openseafeed-control     /usr/local/bin/
COPY --from=builder /app/target/release/openseafeed-worker      /usr/local/bin/

USER osf

# No default CMD: each service sets its own `command`
# (e.g. ["openseafeed-ingest"] or ["openseafeed-worker","connect", ...]).
