#!/bin/sh
# PGO build of the engine (used by the Dockerfile; host-agnostic so it can be tested natively).
# Usage: pgo_build.sh ENGINE_DIR TRAINGEN_DIR OUT_BIN [extra RUSTFLAGS]
set -eu
ENGINE="$1"; TG="$2"; OUT="$3"; EXTRA="${4:-}"
W="${PGO_WORK:-/tmp/pgo-work}"
rm -rf "$W"; mkdir -p "$W/prof"
HOST=$(rustc -vV | sed -n 's/^host: //p')
PROFDATA="$(rustc --print sysroot)/lib/rustlib/$HOST/bin/llvm-profdata"
test -x "$PROFDATA" || { echo "llvm-profdata missing (rustup component add llvm-tools)" >&2; exit 1; }
# 1. our own synthetic training data
(cd "$TG" && cargo build --release --quiet)
"$TG/target/release/traingen" "$W/data" 2000000
# 2. instrumented build + training run
(cd "$ENGINE" && RUSTFLAGS="$EXTRA -Cprofile-generate=$W/prof" cargo build --release --locked --quiet --target-dir "$W/t-gen")
DATA_DIR="$W/data" OUTPUT_DIR="$W/out" "$W/t-gen/release/ss" 2>/dev/null
"$PROFDATA" merge -o "$W/merged.profdata" "$W/prof"
# 3. optimized build with the profile
(cd "$ENGINE" && RUSTFLAGS="$EXTRA -Cprofile-use=$W/merged.profdata" cargo build --release --locked --quiet --target-dir "$W/t-use")
cp "$W/t-use/release/ss" "$OUT"
echo "pgo build ok: $OUT" >&2
