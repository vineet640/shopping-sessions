#!/bin/sh
# hf.sh LABEL CMD... : hyperfine summary line (wall min/median, cpu cycles, instructions)
L="$1"; shift
J=$(mktemp)
hyperfine -w 2 -r "${RUNS:-15}" -N --export-json "$J" "$*" >/dev/null 2>&1
python3 - "$J" "$L" <<'PY'
import json,sys
s=json.load(open(sys.argv[1]))['results'][0]['summary']
w=s['time_wall_clock']
print(f"{sys.argv[2]:<14} wall min {w['min']*1e3:7.1f} med {w['median']*1e3:7.1f} ms | cycles {s['cpu_cycles']['median']/1e9:6.2f} G | instr {s['instructions']['median']/1e9:6.2f} G | cpu {s['time_cpu']['median']:.2f}s")
PY
rm -f "$J"
