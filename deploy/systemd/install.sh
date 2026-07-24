#!/usr/bin/env bash
# Install the OpenSeaFeed connector systemd unit + an env file for one feed.
#
# Usage:
#   sudo ./install.sh <upstream> [path-to-openseafeed-worker]
#
# Examples:
#   sudo ./install.sh norway
#   sudo ./install.sh finland /path/to/openseafeed-worker
#
# By default the binary is taken from ./target/release/openseafeed-worker
# (relative to the repo root) or from $PWD if not found there. After install,
# edit /etc/openseafeed/<upstream>.env to set your feed key, then the unit is
# started with `systemctl enable --now`.
set -euo pipefail

UPSTREAM="${1:-}"
if [[ -z "$UPSTREAM" ]]; then
  echo "usage: $0 <upstream> [path-to-openseafeed-worker]" >&2
  echo "  <upstream> is one of: norway, finland, denmark, ..." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Locate the worker binary.
BIN="${2:-}"
if [[ -z "$BIN" ]]; then
  for cand in \
    "$REPO_ROOT/target/release/openseafeed-worker" \
    "$PWD/openseafeed-worker"; do
    if [[ -x "$cand" ]]; then BIN="$cand"; break; fi
  done
fi
if [[ -z "$BIN" || ! -x "$BIN" ]]; then
  echo "error: openseafeed-worker binary not found." >&2
  echo "  build it (cargo build --release -p openseafeed-worker) or pass a path." >&2
  exit 1
fi

# Root required to write into /usr/local/bin, /etc, and the unit dir.
if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: run as root (sudo)." >&2
  exit 1
fi

ENV_SRC="$SCRIPT_DIR/$UPSTREAM.env"
if [[ ! -f "$ENV_SRC" ]]; then
  echo "warning: no example env for '$UPSTREAM' at $ENV_SRC; using norway.env as a template." >&2
  ENV_SRC="$SCRIPT_DIR/norway.env"
fi

echo "Installing binary -> /usr/local/bin/openseafeed-worker"
install -m 0755 "$BIN" /usr/local/bin/openseafeed-worker

echo "Installing unit  -> /etc/systemd/system/openseafeed-connector@.service"
install -m 0644 "$SCRIPT_DIR/openseafeed-connector@.service" \
  /etc/systemd/system/openseafeed-connector@.service

install -d -m 0755 /etc/openseafeed
ENV_DST="/etc/openseafeed/$UPSTREAM.env"
if [[ -f "$ENV_DST" ]]; then
  echo "Keeping existing $ENV_DST (not overwriting)."
else
  echo "Installing env   -> $ENV_DST"
  install -m 0600 "$ENV_SRC" "$ENV_DST"
fi

systemctl daemon-reload

echo
echo "Next steps:"
echo "  1. Edit $ENV_DST — set OSF_FEED_KEY (and OSF_INGEST_URL / OSF_DENMARK_ADDR as needed)."
echo "  2. Start it:"
echo "       sudo systemctl enable --now openseafeed-connector@$UPSTREAM"
echo "  3. Watch logs:"
echo "       journalctl -u openseafeed-connector@$UPSTREAM -f"
