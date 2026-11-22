# Submissions (2/hour/team, >=31 min apart)

| time (CST) | SHA | change | result |
| --- | --- | --- | --- |
| Sat 16:01 | 743aa78 | Polars v1 first correct | BB-D5F6CBDC: passed dev, REJECTED OOM on 20M timed run |
| Sat 16:32 | 36e3245 | Rust engine first submission | BB-D0FD2D0E: SCORED 2.87 s (best) |
| Sat 17:03 | e74e3f4 | perf round 1 (-21% on 5M) | BB-D767DF15: SCORED 2.47 s (best) |
| Sat 17:34 | 01990d8 | engine v2 (links-first, exact keys, bitmask tokenizer, radix) | BB-70117C00: SCORED 2.1757 s (best) |
| Sat 18:04 | bf89737 | v2 + parallel chunk snapping, id prefetch, span assists | BB-90F25A87: scored 2.1974 s (no gain vs 2.1757; chunk-scan fix did not matter on scorer) |
| Sat 18:34 | 5c695dd (diag branch) | DIAGNOSTIC: scorer facts + 20M stage timings, fails dev on purpose | BB-5C8ADE62: rejected on purpose; logs: /data=tmpfs (cat 92MB 17ms), dev wall 137ms vs internal 124ms, links stage 43ms on dev (~0.25s at 20M?) |
| Sat 19:04 | 7e932df | fused dedup, radix_gather, SWAR ts, concurrent UF, THP table, branch-light tokenizer, x86-64-v2, person sort | BB-7548E45A: SCORED 1.8910 s (best) |
| Sat 19:34 | 983bc4f (diag) | DIAGNOSTIC v2: CPU flags, THP, 20M stage timings x7 variants | BB-BA6F4D24: rejected on purpose. Xeon 2.8GHz AVX2+BMI2+AVX512, THP=madvise, L3 33MB. 20M wall 1.70s: links .14, parse ~1.0, dedup .07, sessions .29, write .02; populate none 1.69 vs map 1.72 vs chunk 1.76; NCPU16 1.80, NCPU4 2.14 |
| Sat 20:04 | 7de0291 (=1c9d642 code) | bucket slabs (THP) + prefetch ring + v3 + 32B clicks + streaming parquet | BB-1622F2AA: SCORED 1.5656 s (best; slabs+ring+v3 confirmed TLB/latency hypothesis) |
| Sat 20:34 | 0f3742b | + huge-page SmallMap/dedup/name/UF/sort buffers, deadlock fix, interleaved counters | BB-9462F329: SCORED 1.4784 s (best) |
| Sat 21:04 | e62793b | PGO build (own traingen) + single-pass person radix + F3 SWAR + hash-once rings + dedup hash ring | BB-083322C0 queued |
