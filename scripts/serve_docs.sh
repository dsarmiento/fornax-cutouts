#!/usr/bin/env bash
# Serve documentation locally with live reload.
# Requires: npm / npx (mystmd is installed on-demand via npx)
#
# Usage: ./scripts/serve_docs.sh [--port PORT]
set -euo pipefail

cd "$(dirname "$0")/.."

npx mystmd start "$@"
