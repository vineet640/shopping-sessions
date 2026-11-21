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
- 2026-11-21 16:15 CST: Polars v1 (743aa78) was REJECTED: out of memory on the 20M practice run. Skipped patching it; went to the Rust engine (engine/) right away. Old Dockerfile kept as pipeline/Dockerfile.polars.
- Rust engine design: mmap + parallel chunk parse with a strict hand-written dialect parser; IDs interned in a lock-free open-addressing table keyed by 128-bit xxh3 (collision odds ~1e-26, accepted as exact); event dedup by 128-bit hash of event_id in 1024 hash buckets (sort by hash, ingest); union-find over table slots, person name = byte-wise smallest ID string; per-person buckets (512) sort by (root, ts) -> sessions, credit (sliding window, ties by click_id bytes), assists (sliding active-campaign set).
- Clicks are NOT deduped: rules say copies are identical, and identical copies change neither credit nor assists.
- ID strings are copied into per-thread arenas when first interned: comparing names through the CSV mmaps caused 30 s of page faults under local memory pressure.
- Dockerfile: dependency-cache layer needs `touch src/main.rs` (COPY keeps the older mtime -> cargo shipped the dummy main). The build now asserts the binary is >1 MB.
- target-cpu left at the default x86-64 baseline (no SIGILL risk on unknown scorer CPUs).
