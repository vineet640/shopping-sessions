#!/bin/sh
# DIAGNOSTIC ENTRY v2 (branch `diag` only): compact scorer facts + 20M stage timings on stderr,
# then exit 1 on purpose so the dev-set check fails and returns these logs (log window ~3 KB).
exec 1>&2
ns() { date +%s%N; }
echo "CPU:$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) | n=$(nproc) | THP=$(cat /sys/kernel/mm/transparent_hugepage/enabled) | L3:$(grep -m1 'cache size' /proc/cpuinfo | cut -d: -f2)"
echo "FLAGS: $(grep -m1 '^flags' /proc/cpuinfo | tr ' ' '\n' | grep -xE 'avx2|bmi1|bmi2|avx512f|avx512bw|avx512vl|popcnt|lzcnt|movbe|fma' | tr '\n' ' ')"
t0=$(ns); /app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g >/dev/null 2>&1; t1=$(ns); echo "gen20M $(( (t1-t0)/1000000 ))ms"
run() {
  rm -rf /scratch/o; mkdir -p /scratch/o
  t0=$(ns); env DATA_DIR=/scratch/g OUTPUT_DIR=/scratch/o "$@" /app/ss 2>/scratch/log; t1=$(ns)
  echo "$* wall=$(( (t1-t0)/1000000 ))ms: $(sed -E 's/\] ([A-Za-z0-9-]+).*/ \1/; s/\[ *//; s/s / /' /scratch/log | tr '\n' ' ')"
}
run X=1 SS_STATS=1
run X=2 SS_STATS=1
run X=3
run SS_POPMODE=map
run SS_POPMODE=chunk
echo "END (exit 1 on purpose)"
exit 1
