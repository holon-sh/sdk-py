#!/bin/sh
set -eu

# Run from any directory with the holon CLI on PATH. The orders server is
# statically extracted; no running server or installed fixture dependencies are needed.
cd "$(dirname "$0")/../tests/fixtures/caller"
exec holon types generate -s ../orders
