# Submissions (2/hour/team, >=31 min apart)

| time (CST) | SHA | change | result |
| --- | --- | --- | --- |
| Sat 16:01 | 743aa78 | Polars v1 first correct | BB-D5F6CBDC: passed dev, REJECTED OOM on 20M timed run |
| Sat 16:32 | 36e3245 | Rust engine first submission | BB-D0FD2D0E: SCORED 2.87 s (best) |
| Sat 17:03 | e74e3f4 | perf round 1 (-21% on 5M) | BB-D767DF15: SCORED 2.47 s (best) |
| Sat 17:34 | 01990d8 | engine v2 (links-first, exact keys, bitmask tokenizer, radix) | BB-70117C00: SCORED 2.1757 s (best) |
| Sat 18:04 | bf89737 | v2 + parallel chunk snapping, id prefetch, span assists | BB-90F25A87: scored 2.1974 s (no gain vs 2.1757; chunk-scan fix did not matter on scorer) |
| Sat 18:34 | 5c695dd (diag branch) | DIAGNOSTIC: scorer facts + 20M stage timings, fails dev on purpose | BB-5C8ADE62 queued |
