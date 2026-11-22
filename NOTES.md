# NOTES

- Phase: 4 FAST. HEAD 9569e74 (engine v2 + fused dedup + radix_gather + SWAR ts + prefetch). Gates: fixtures, dev, difftest, 20M equivalence, x86 cargo check. No local Docker (laptop disk) -> scorer dev check is the container gate.
- Best scored: 01990d8 = 2.1757 s (BB-70117C00). bf89737 = 2.1974 s.
- Local timings (M-series, 8 thr): 5M ~0.245 s; 10M ~0.57 s (links .05, parse .36, dedup .02, sessions .10, write .05).
- Next: diag submission (branch diag, e841f8c) at 18:34 -> read logs -> act on scorer facts (THP, populate, threads, stage split). Submit HEAD at 19:04.
- Tools: scripts/hf.sh (hyperfine summary), scripts/xtprof.py (xctrace profile summary), scripts/run_rust.sh, SS_CHUNK, SS_NOPOPULATE, `cargo test --release ts_fuzz`.
- Cutoff Sun 10:00 CST (site shows ET). Endgame per CLAUDE.md.
