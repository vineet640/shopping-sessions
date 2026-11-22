# NOTES

- Phase: 4 FAST. HEAD aa1c338 (PGO Dockerfile, huge pages everywhere random, prefetch rings, single-pass person radix, batched names/parquet). Quota 5/h since 21:12 (12-min spacing).
- Best scored: e62793b = 1.4165 s (BB-083322C0, 21:04, PGO). Earlier: 0f3742b 1.4784, 7de0291 1.5656, 7e932df 1.891.
- Local timings (M-series, 8 thr): 5M ~0.245 s; 10M ~0.57 s (links .05, parse .36, dedup .02, sessions .10, write .05).
- Next: read diag v3 (BB-6E258916) stage split; submit HEAD ~21:25; verify 60M (regenerating data/s60000000_9) vs v1 output.
- Tools: scripts/hf.sh (hyperfine summary), scripts/xtprof.py (xctrace profile summary), scripts/run_rust.sh, SS_CHUNK, SS_NOPOPULATE, `cargo test --release ts_fuzz`.
- Cutoff Sun 10:00 CST (site shows ET). Endgame per CLAUDE.md.
