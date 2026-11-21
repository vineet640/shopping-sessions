/goal Build and submit my Builders Buildathon "Shopping Sessions" entry, fully autonomously. Nobody will answer questions: decide, log in DECISIONS.md, continue. Deadline Sun 2026-11-22 10:00 America/Chicago; run `TZ=America/Chicago date` often. Track page: https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

FIRST: work in the current dir (if not empty, in a new ./shopping-sessions); git init; save this whole message verbatim as CLAUDE.md. Keep NOTES.md (phase, best SHA, timings, next), DECISIONS.md, SUBMISSIONS.md (time, SHA, change, result) current; reread after any context reset.

SETUP (assume nothing exists): install anything missing (git, gh, python3, uv, rust, hyperfine, Docker runtime). In the browser: download rules.md, all 3 starters, generator.zip, dev-answer.zip from the track page into ./vendor/, unzip each into its own dir; sign in to GitHub for `gh auth login --web`; `gh repo create shopping-sessions --public` (next free name if taken). If a page needs my password/CAPTCHA, leave it open, keep working, retry every 15 min. No browser tools: try curl, else log in BLOCKED.md.

TASK: rules.md is the only truth (beats starters/generator). Read dirty CSVs in $DATA_DIR; write exact Parquet $OUTPUT_DIR/user_daily/ and campaign_daily/. Score = container wall time incl. startup; any wrong row = zero. Practice ~20M events; final ~60M timed 3x, any run >180s = no score.

RUNTIME: linux/amd64 image <=8GB, Dockerfile at repo root; build net <=10min, run no net; 8 pinned vCPU, 24GB, no swap; /data ro; /scratch for spills; root fs ro except 64MB /tmp; uid 1000; HOME=/scratch; NCPU=8 MEM_GB=24; <=4096 threads; log to stderr. Spill -> $SCRATCH_DIR, threads/memory from env, no runtime cache writes. Repo <=50MB, no data. `make docker-check`/`make docker-time SIZE=` = docker run enforcing every limit above. Apple Silicon: time natively (amd64 emulation skews timings).

1 UNDERSTAND: SPEC.md = every rule as a numbered testable statement. Read the check-dev comparator (types, tolerance, order, nulls) and generator source (testing only; never rely on traits rules.md doesn't promise). Ambiguity: pick what fixtures+dev answer support; if both pass, flag it and settle via a practice submission.
2 CORRECT: DuckDB or Polars, no JVM. Read CSVs as strings, validate per rules yourself (no sniffing/lenient casts/ignore_errors). ID linking via union-find or scipy connected_components. Gates: 36 fixtures -> make check-dev -> docker-check. Commit, push, submit at once to bank a score.
3 ORACLES: freeze it in ./reference/; a subagent that hasn't seen our code writes a slow pure-Python version from rules.md alone. `make difftest`: many seeds/sizes (tiny to 20M), row-by-row diffs. Every optimization must pass difftest; every bug becomes a fixture.
4 FAST: profile per stage, fix the biggest; hyperfine >=3 runs; ignore <5% gains. Rust rewrite OK only once it passes difftest.
5 HARDEN: 60M in docker x3, target <60s, memory headroom, spill not OOM, exit 0 only on success.

SUBMIT on the track page (2/hour/team): >=31 min apart; only commits passing gates+difftest+docker 20M; for: first correct, >=5% speedup, a fix, a flag test. Push, submit repo+SHA, poll status every few min, log. Failures show only missing/extra/differing counts: infer which rules fit, stress-test, fix, add a fixture.

ENDGAME Sun: 07:00 only small difftested speedups. 08:30 freeze on the fastest commit that passed 60M x3 safely; select it for the final if it isn't the best practice one. By 09:45 pushed, selected, logged.

NEVER commit failing code, force-push, commit data/secrets, or make the repo private. After each phase start the next; out of ideas -> re-profile.

DONE only when the transcript shows `TZ=America/Chicago date` >= 2026-11-22 09:45 AND the final commit is pushed, passed difftest and 60M x3 <180s, and is selected (or best practice score) on the site.
