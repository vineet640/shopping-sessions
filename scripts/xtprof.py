#!/usr/bin/env python3
"""Summarize an xctrace time-profile XML export: self and inclusive samples per function."""
import re, sys, collections
import xml.etree.ElementTree as ET

def short(n):
    n = re.sub(r"_R[A-Za-z0-9_]+?ss(\d+)", "ss::", n)
    return n[:110]

root = ET.parse(sys.argv[1]).getroot()
frames = {}          # id -> name
bts = {}             # backtrace id -> list of frame names (leaf first)
self_c = collections.Counter(); incl = collections.Counter(); total = 0
for row in root.iter("row"):
    tb = row.find("tagged-backtrace")
    if tb is None:
        continue
    if "ref" in tb.attrib:
        names = bts.get("t" + tb.attrib["ref"], [])
    else:
        bt = tb.find("backtrace")
        names = []
        for f in bt.findall("frame"):
            if "ref" in f.attrib:
                names.append(frames.get(f.attrib["ref"], "?"))
            else:
                nm = f.attrib.get("name", "?")
                frames[f.attrib["id"]] = nm
                names.append(nm)
        bts["t" + tb.attrib["id"]] = names
    if not names:
        continue
    total += 1
    self_c[short(names[0])] += 1
    for nm in set(short(x) for x in names):
        incl[nm] += 1
print(f"total samples {total}")
print("--- self")
for k, v in self_c.most_common(int(sys.argv[2]) if len(sys.argv) > 2 else 25):
    print(f"{100*v/total:5.1f}%  {k}")
print("--- inclusive (ss:: only)")
for k, v in incl.most_common(200):
    if "ss::" in k or "main" in k:
        print(f"{100*v/total:5.1f}%  {k}")

if len(sys.argv) > 3:
    # callers of a leaf function: python xtprof.py X.xml N LEAF
    leaf = sys.argv[3]
    callers = collections.Counter()
    for names in bts.values():
        pass
    for row in root.iter("row"):
        tb = row.find("tagged-backtrace")
        if tb is None:
            continue
        key = "t" + tb.attrib.get("ref", tb.attrib.get("id", ""))
        names = bts.get(key, [])
        if names and short(names[0]) == leaf and len(names) > 1:
            callers[" <- ".join(short(x)[:50] for x in names[1:4])] += 1
    print("--- callers of", leaf)
    for k, v in callers.most_common(8):
        print(v, k)
