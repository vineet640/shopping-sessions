# NOTES

- Phase: 4 FAST (Rust engine). Gates passed: 36/36 fixtures native+docker, dev MATCH native+docker, difftest PASS (2k/20k/200k x 8 seeds), docker 20M (MEM=3g, emulated) == native.
- Best SHA: (pending first Rust submission)
- Timings (Rust): dev native 0.17 s; 20M native 4.5 s (Mac short on RAM: parse 3 s); 20M docker emulated 19 s.
- Next: submit, then profile parse (biggest stage), bigger difftest sizes (2M), 60M local test.
- Local tools: bin/generate (gitignored), .venv, rustup via brew (PATH="$(brew --prefix rustup)/bin:$HOME/.cargo/bin:$PATH"). scripts/run_rust.sh = native Rust run; IMPL=$PWD/scripts/run_rust.sh scripts/difftest.sh.
