# NOTES

- Phase: 2 CORRECT done (Polars v1): fixtures 36/36 native, dev MATCH native + amd64 docker.
- Best SHA: (none scored yet)
- Timings: dev native 4.3 s (M-series, 8 threads); dev docker amd64-emulated 17.5 s.
- Next: commit, push, submit; difftest; profile; Rust rewrite.
- Local tools: bin/generate (gitignored), .venv (polars, numpy, scipy, pyarrow), rustup via brew (PATH="$(brew --prefix rustup)/bin:$HOME/.cargo/bin:$PATH").
