#!/usr/bin/env bash
# Build documentation for deployment.
# Requires: npm / npx (mystmd is installed on-demand via npx)
#
# Usage:
#   ./scripts/build_docs.sh           # build HTML site
#   ./scripts/build_docs.sh --pdf     # build PDF (requires LaTeX)
#
# Output is written to _build/html/
set -euo pipefail

cd "$(dirname "$0")/.."

npx mystmd build --html "$@"

echo "Done. Output is in _build/html/"
