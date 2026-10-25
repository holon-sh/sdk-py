#!/bin/sh
# Regenerates tests/fixtures/* with the holon CLI's compiler:
#   HOLON=/path/to/holon sh scripts/fixtures.sh   (default: `holon` on PATH; uv tool install --prerelease allow holon-types)
set -eu
cd "$(dirname "$0")/../tests/fixtures"
for request in *.request.json; do
  name=${request%.request.json}
  rm -rf "$name" && mkdir -p "$name"
  "${HOLON:-holon}" types compile "$request" --out "$name"
done
