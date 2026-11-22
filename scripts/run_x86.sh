#!/bin/sh
# x86_64 build of the engine run under Rosetta 2 (tests the SSE2/AVX2 code paths on Apple Silicon).
# Build first: (cd engine && RUSTFLAGS="-C target-cpu=x86-64-$V" cargo build --release --target x86_64-apple-darwin --target-dir target/x86$V)
# Usage: X86V=v2|v3 scripts/run_x86.sh DATA_DIR OUTPUT_DIR
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
V="${X86V:-v2}"
DATA_DIR="$1" OUTPUT_DIR="$2" NCPU="${NCPU:-8}" exec "$ROOT/engine/target/x86$V/x86_64-apple-darwin/release/ss"
