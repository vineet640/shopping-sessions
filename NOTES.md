# NOTES

- Phase: 4 FAST. Engine v2 (exact keys, links-first, bitmask tokenizer, radix sort) passes all gates incl. docker + 60M==v1.
- Best SHA: 01990d8 = 2.1757 s practice (BB-70117C00, Sat 17:34 CST)
- Timings (Rust): dev native 0.17 s; 20M native 4.5 s (Mac short on RAM: parse 3 s); 20M docker emulated 19 s.
- Next: BB-90F25A87 (bf89737) scoring; diag e086bbc at next slot; then optimize from scorer profile. Cutoff Sun 10:00 CST (site shows ET).
- Local tools: bin/generate (gitignored), .venv, rustup via brew (PATH="$(brew --prefix rustup)/bin:$HOME/.cargo/bin:$PATH"). scripts/run_rust.sh = native Rust run; IMPL=$PWD/scripts/run_rust.sh scripts/difftest.sh.
