#!/usr/bin/env bash
# End-to-end smoke test wrapper: sets up a throwaway venv for the Python
# test deps, then runs scripts/e2e.py against a running stack (make dev,
# or services started manually). See e2e.py for what is asserted.
set -euo pipefail
cd "$(dirname "$0")/.."

VENV=.e2e-venv
if [ ! -x "$VENV/bin/python" ]; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" -q install websockets requests
fi
exec "$VENV/bin/python" scripts/e2e.py "$@"
