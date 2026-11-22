# DIAGNOSTIC image (branch `diag`): PGO engine (as main) + official generator + diag.sh. Fails the dev check on purpose.
FROM --platform=linux/amd64 rust:1-bookworm AS build
RUN rustup component add llvm-tools
WORKDIR /src
COPY engine/Cargo.toml engine/Cargo.lock engine/
COPY engine/src engine/src
COPY tools/traingen tools/traingen
COPY scripts/pgo_build.sh scripts/pgo_build.sh
RUN sh scripts/pgo_build.sh engine tools/traingen /ss "-C target-cpu=x86-64-v3" && strip /ss && rm -rf /tmp/pgo-work
COPY tools/generator /gen
RUN cd /gen && cargo build --release && cp target/release/generate /generate

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /ss /app/ss
COPY --from=build /generate /app/generate
COPY diag.sh /app/diag.sh
WORKDIR /app
CMD ["/app/diag.sh"]
