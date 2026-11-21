#!/bin/sh
# difftest.sh: run the pipeline and the independent oracle on many generated
# datasets (seeds x sizes) and diff row-by-row. Usage: difftest.sh [SIZES] [SEEDS]
# Env: IMPL (command taking DATA OUT, default scripts/run_local.sh), ORACLE_MAX (largest size the oracle runs on).
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
SIZES="${1:-2000 20000 200000}"
SEEDS="${2:-1 2 3 4 5 6 7 8}"
IMPL="${IMPL:-$ROOT/scripts/run_local.sh}"
PY="$ROOT/.venv/bin/python"
T="$ROOT/data/difftest"; mkdir -p "$T"
fail=0
for n in $SIZES; do for s in $SEEDS; do
  D=$("$ROOT/scripts/gen.sh" "$n" "$s")
  O="$T/oracle_${n}_${s}"
  if [ ! -d "$O/user_daily" ]; then rm -rf "$O"; DATA_DIR="$D" OUTPUT_DIR="$O" "$PY" -I "$ROOT/reference/oracle.py" "$D" "$O" 2>/dev/null; fi
  rm -rf "$T/impl"; "$IMPL" "$D" "$T/impl" 2>/dev/null
  if (cd "$ROOT/scripts" && "$PY" compare.py "$T/impl" "$O" > "$T/last.txt"); then echo "ok   n=$n seed=$s"; else echo "DIFF n=$n seed=$s"; cat "$T/last.txt"; fail=1; fi
done; done
[ $fail = 0 ] && echo "DIFFTEST PASS" || { echo "DIFFTEST FAIL"; exit 1; }
