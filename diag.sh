#!/bin/sh
# DIAGNOSTIC ENTRY (branch `diag` only): prints scorer facts and timings to stderr, then exits 1
# on purpose so the dev-set check fails and returns these logs.
exec 1>&2
ns() { date +%s%N; }
ms() { echo $(( ($2 - $1) / 1000000 )); }
echo "=== DIAG uname: $(uname -a)"
echo "=== nproc: $(nproc)  NCPU=$NCPU MEM_GB=$MEM_GB"
grep -m1 "model name" /proc/cpuinfo; grep -m1 "cache size" /proc/cpuinfo; grep -m1 flags /proc/cpuinfo | tr ' ' '\n' | grep -E '^(avx2|avx512f|avx512bw|bmi2|sse4_2)$' | tr '\n' ' '; echo
echo "THP: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>&1) defrag: $(cat /sys/kernel/mm/transparent_hugepage/defrag 2>&1)"
head -3 /proc/meminfo
cat /sys/fs/cgroup/cpu.max 2>/dev/null; cat /sys/fs/cgroup/memory.max 2>/dev/null
grep -E " /data | /scratch | /output | / " /proc/mounts
df -h /data /scratch 2>&1 | tail -2
du -sh /data 2>&1
t0=$(ns); cat /data/events/*.csv /data/clicks/*.csv /data/links/*.csv > /dev/null; t1=$(ns); echo "cat /data: $(ms $t0 $t1) ms"
t0=$(ns); cat /data/events/*.csv /data/clicks/*.csv /data/links/*.csv > /dev/null; t1=$(ns); echo "cat /data again: $(ms $t0 $t1) ms"
t0=$(ns); /app/ss_noop 2>/dev/null; t1=$(ns); echo "noop process: $(ms $t0 $t1) ms"
# engine on the dev set
for i in 1 2 3; do
  rm -rf /scratch/od; mkdir -p /scratch/od
  t0=$(ns); OUTPUT_DIR=/scratch/od /app/ss; t1=$(ns); echo "### dev run $i wall: $(ms $t0 $t1) ms"
done
# generate 20M events into /scratch with the official generator
t0=$(ns); /app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g10 >/dev/null 2>&1; t1=$(ns); echo "generate 20M: $(ms $t0 $t1) ms"
du -sh /scratch/g10
for i in 1 2 3; do
  rm -rf /scratch/o10; mkdir -p /scratch/o10
  t0=$(ns); DATA_DIR=/scratch/g10 OUTPUT_DIR=/scratch/o10 /app/ss; t1=$(ns); echo "### 20M run $i wall: $(ms $t0 $t1) ms"
done
rm -rf /scratch/o10; mkdir -p /scratch/o10
t0=$(ns); DATA_DIR=/scratch/g10 OUTPUT_DIR=/scratch/o10 SS_NOPOPULATE=1 /app/ss; t1=$(ns); echo "### 20M NOPOPULATE wall: $(ms $t0 $t1) ms"
rm -rf /scratch/o10; mkdir -p /scratch/o10
t0=$(ns); DATA_DIR=/scratch/g10 OUTPUT_DIR=/scratch/o10 NCPU=4 /app/ss; t1=$(ns); echo "### 20M NCPU=4 wall: $(ms $t0 $t1) ms"
rm -rf /scratch/o10; mkdir -p /scratch/o10
t0=$(ns); DATA_DIR=/scratch/g10 OUTPUT_DIR=/scratch/o10 NCPU=16 /app/ss; t1=$(ns); echo "### 20M NCPU=16 wall: $(ms $t0 $t1) ms"
echo "=== DIAG END (exit 1 on purpose)"
exit 1
