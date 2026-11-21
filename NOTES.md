# NOTES

- Phase: 4 FAST. Engine v2 (exact keys, links-first, bitmask tokenizer, radix sort) passes all gates incl. docker + 60M==v1.
- Best SHA: 01990d8 = 2.1757 s practice (BB-70117C00, Sat 17:34 CST)
- Timings (Rust): dev native 0.17 s; 20M native 4.5 s (Mac short on RAM: parse 3 s); 20M docker emulated 19 s.
- Next: poll BB-D0FD2D0E; profile parse; 60M local test. ENDGAME shifted 1h earlier (see DECISIONS 16:32): freeze 07:30, done 08:45 CST.
- Local tools: bin/generate (gitignored), .venv, rustup via brew (PATH="$(brew --prefix rustup)/bin:$HOME/.cargo/bin:$PATH"). scripts/run_rust.sh = native Rust run; IMPL=$PWD/scripts/run_rust.sh scripts/difftest.sh.
