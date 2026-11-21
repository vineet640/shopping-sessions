# Decisions (autonomous; nobody to ask)

- 2026-11-21 15:48 CST: Working dir was empty -> repo at ~/shopping-sessions. Branch `main`.
- Track files fetched with curl (public URLs found on the track page); no login needed. vendor/ is gitignored.
- GitHub: `gh` was already authenticated (account vineet640) -> skipped `gh auth login --web`.
- Machine: Apple M-series, 8 cores, 8 GB RAM, ~18 GB free disk; Docker VM has 4 GB. Local docker runs use MEM=3g (Makefile default), not 24g. 60M local runs will be native (and/or with reduced data) — limited by RAM/disk.
- First line of each CSV is always the header and is skipped; later identical lines are data (fixture L3).
- Engine v1: Polars 2.1.0 + numpy searchsorted for credit/assists + scipy connected_components for links. Hand-parsed CSV (split on \n and ',', strict quote rule), regex [0-9] for timestamps.
- Polars' default x86 runtime segfaults under Docker Desktop amd64 emulation on Apple Silicon (even `pl.DataFrame({'a':[1]})`). Installed `polars[rtcompat]` so the image works under emulation; Polars picks the best runtime for the CPU, so on the Xeon scorer it should use the fast one.
- Oracle: reference/oracle.py written by an independent subagent from rules.md + fixtures only; 36/36 fixtures, dev MATCH. Its ambiguity list is at the top of the file (none conflicts with ours).
- Clicks of people with no surviving events are dropped early (they can't credit or assist anything).
