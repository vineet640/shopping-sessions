#!/bin/sh
# DIAGNOSTIC A/B (branch `diag` only): alternate baseline vs current engine on one 20M dataset,
# print wall times + stage lines, then exit 1 on purpose so the dev check fails and returns logs.
exec 1>&2
ns() { date +%s%N; }
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g >/dev/null 2>&1
run() {
  B="$1"; shift
  rm -rf /scratch/o; mkdir -p /scratch/o
  t0=$(ns); env DATA_DIR=/scratch/g OUTPUT_DIR=/scratch/o "$@" /app/$B 2>/scratch/log; t1=$(ns)
  echo "$B $* wall=$(( (t1-t0)/1000000 ))ms: $(grep -v stats /scratch/log | sed -E 's/\] ([A-Za-z0-9-]+).*/ \1/; s/\[ *//; s/s / /' | tr '\n' ' ')"
  grep stats /scratch/log | sed -E 's/^\[ *[0-9.]+s\] stats //'
}
run ss_base
run ss_new
run ss_base
run ss_new
run ss_base
run ss_new SS_STATS=1
echo "END (exit 1 on purpose)"
exit 1
