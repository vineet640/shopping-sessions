# Rust engine (engine/). Build stage compiles; runtime stage only holds the binary.
FROM --platform=linux/amd64 rust:1-bookworm AS build
WORKDIR /src
COPY engine/Cargo.toml engine/Cargo.lock ./
# cache dependencies
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release --locked && rm -rf src target/release/deps/ss-* target/release/ss* target/release/.fingerprint/ss-*
COPY engine/src ./src
RUN touch src/main.rs && cargo build --release --locked && strip target/release/ss && test $(stat -c %s target/release/ss) -gt 1000000

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /src/target/release/ss /app/ss
COPY run.sh /app/run.sh
WORKDIR /app
CMD ["/app/run.sh"]
