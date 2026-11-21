# NOTES

- Phase: 4 FAST (Rust engine). Gates passed: 36/36 fixtures native+docker, dev MATCH native+docker, difftest PASS (2k/20k/200k x 8 seeds), docker 20M (MEM=3g, emulated) == native.
- Best SHA: 36e3245 = 2.87 s practice (BB-D0FD2D0E, Sat 16:32 CST)
- Timings (Rust): dev native 0.17 s; 20M native 4.5 s (Mac short on RAM: parse 3 s); 20M docker emulated 19 s.
- Next: poll BB-D0FD2D0E; profile parse; 60M local test. ENDGAME shifted 1h earlier (see DECISIONS 16:32): freeze 07:30, done 08:45 CST.
- Local tools: bin/generate (gitignored), .venv, rustup via brew (PATH="$(brew --prefix rustup)/bin:$HOME/.cargo/bin:$PATH"). scripts/run_rust.sh = native Rust run; IMPL=$PWD/scripts/run_rust.sh scripts/difftest.sh.
