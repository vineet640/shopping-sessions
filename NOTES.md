# NOTES

- Phase: 4 FAST. HEAD aa1c338 (PGO Dockerfile, huge pages everywhere random, prefetch rings, single-pass person radix, batched names/parquet). Quota 5/h since 21:12 (12-min spacing).
- Best scored: f9eff02 = 1.3173 s (BB-0A2D8C48, 21:24). Earlier: e62793b 1.4165 (PGO), 0f3742b 1.4784, 7de0291 1.5656.
- Local timings (M-series, 8 thr): 5M ~0.245 s; 10M ~0.57 s (links .05, parse .36, dedup .02, sessions .10, write .05).
- Next: read BB-F8DBAF42 (43108d9, PGO + inlining); submit HEAD (single-call-site tokenizer) next slot; A/B via diag branch harness (engine_base vs engine). Endgame per CLAUDE.md: 07:00 small speedups only, 08:30 freeze best 60M-verified commit, 09:45 done.
- Tools: scripts/hf.sh (hyperfine summary), scripts/xtprof.py (xctrace profile summary), scripts/run_rust.sh, SS_CHUNK, SS_NOPOPULATE, `cargo test --release ts_fuzz`.
- Cutoff Sun 10:00 CST (site shows ET). Endgame per CLAUDE.md.
