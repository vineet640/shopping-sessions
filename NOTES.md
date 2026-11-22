# NOTES

- Phase: 4 FAST. HEAD 1c9d642 (slabs) -> submit 20:04. Scorer profile (diag v2, 20M): parse ~60%, sessions ~18%, links ~8%, dedup ~4%.
- Best scored: e62793b = 1.4165 s (BB-083322C0, 21:04, PGO). Earlier: 0f3742b 1.4784, 7de0291 1.5656, 7e932df 1.891.
- Local timings (M-series, 8 thr): 5M ~0.245 s; 10M ~0.57 s (links .05, parse .36, dedup .02, sessions .10, write .05).
- Next: submit 1c9d642; if parse drops -> TLB hypothesis confirmed; else revert slabs. Then: sessions (person logic, gather/sort, parquet ~equal shares), links.
- Tools: scripts/hf.sh (hyperfine summary), scripts/xtprof.py (xctrace profile summary), scripts/run_rust.sh, SS_CHUNK, SS_NOPOPULATE, `cargo test --release ts_fuzz`.
- Cutoff Sun 10:00 CST (site shows ET). Endgame per CLAUDE.md.
