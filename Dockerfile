# DIAGNOSTIC A/B image (branch `diag`): baseline engine (engine_base/, = f9eff02) vs current engine,
# both built the same way (x86-64-v3, no PGO), timed alternately on one generated 20M dataset.
FROM --platform=linux/amd64 rust:1-bookworm AS build
WORKDIR /src
COPY engine engine
COPY engine_base engine_base
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cd engine && cargo build --release --locked && cp target/release/ss /ss_new
RUN cd engine_base && cargo build --release --locked && cp target/release/ss /ss_base
COPY tools/generator /gen
RUN cd /gen && cargo build --release && cp target/release/generate /generate

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /ss_new /app/ss_new
COPY --from=build /ss_base /app/ss_base
COPY --from=build /generate /app/generate
COPY diag.sh /app/diag.sh
WORKDIR /app
CMD ["/app/diag.sh"]
