#!/bin/sh
# Engine run with a watchdog (macOS has no `timeout`): run_wd.sh SECONDS DATA_DIR OUTPUT_DIR
# Exit 124 and print HUNG if the engine does not finish in time.
S="$1"; shift
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA_DIR="$1" OUTPUT_DIR="$2" NCPU="${NCPU:-8}" "$ROOT/engine/target/release/ss" &
P=$!
i=0
while kill -0 $P 2>/dev/null; do
  i=$((i + 1)); if [ $i -gt $((S * 10)) ]; then echo "HUNG"; kill -9 $P; exit 124; fi
  sleep 0.1
done
wait $P
