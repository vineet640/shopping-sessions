#!/bin/sh
# gen.sh EVENTS SEED -> data/s<EVENTS>_<SEED> (clicks = events/4, like the dev set)
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
D="$ROOT/data/s$1_$2"
[ -f "$D/manifest.json" ] || "$ROOT/bin/generate" --seed "$2" --events "$1" --clicks $(( $1 / 4 )) --output "$D" >&2
echo "$D"
