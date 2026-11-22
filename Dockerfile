# Rust engine (engine/), built with profile-guided optimization:
# our own synthetic training data (tools/traingen) -> instrumented build -> training run -> optimized build.
FROM --platform=linux/amd64 rust:1-bookworm AS build
RUN rustup component add llvm-tools
WORKDIR /src
COPY engine/Cargo.toml engine/Cargo.lock engine/
COPY engine/src engine/src
COPY tools/traingen tools/traingen
COPY scripts/pgo_build.sh scripts/pgo_build.sh
RUN sh scripts/pgo_build.sh engine tools/traingen /ss "-C target-cpu=x86-64-v3" \
 && strip /ss && test $(stat -c %s /ss) -gt 1000000 && rm -rf /tmp/pgo-work

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /ss /app/ss
COPY run.sh /app/run.sh
WORKDIR /app
CMD ["/app/run.sh"]
