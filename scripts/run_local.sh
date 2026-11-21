#!/bin/sh
# Native run of the current pipeline: run_local.sh DATA_DIR OUTPUT_DIR
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA_DIR="$1" OUTPUT_DIR="$2" SCRATCH_DIR="${SCRATCH_DIR:-${TMPDIR:-/tmp}}" NCPU="${NCPU:-8}" MEM_GB="${MEM_GB:-6}" \
  exec "$ROOT/.venv/bin/python" "$ROOT/pipeline/pipeline.py"
