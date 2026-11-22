# DIAGNOSTIC image (branch `diag`): engine + official generator + diag.sh. Fails the dev check on purpose.
FROM --platform=linux/amd64 rust:1-bookworm AS build
WORKDIR /src
COPY engine/Cargo.toml engine/Cargo.lock ./
ENV RUSTFLAGS="-C target-cpu=x86-64-v2"
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release --locked && rm -rf src target/release/deps/ss-* target/release/ss* target/release/.fingerprint/ss-*
COPY engine/src ./src
RUN touch src/main.rs && cargo build --release --locked && strip target/release/ss && test $(stat -c %s target/release/ss) -gt 1000000
COPY tools/generator /gen
RUN cd /gen && cargo build --release && cp target/release/generate /generate
RUN printf 'fn main(){}' > /noop.rs && rustc -O /noop.rs -o /noop

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /src/target/release/ss /app/ss
COPY --from=build /generate /app/generate
COPY --from=build /noop /app/ss_noop
COPY diag.sh /app/diag.sh
WORKDIR /app
CMD ["/app/diag.sh"]
