# syntax=docker/dockerfile:1
FROM rust:1.94.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS broker
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home anvil \
    && mkdir /data && chown anvil:anvil /data
COPY --from=build /build/target/release/rusty-queue /usr/local/bin/anvilmq
USER anvil
ENV ANVILMQ_ADDR=0.0.0.0:50051 ANVILMQ_HTTP_ADDR=0.0.0.0:9090 ANVILMQ_DB_PATH=/data/anvil.db RUST_LOG=info
EXPOSE 50051 9090
VOLUME /data
ENTRYPOINT ["/usr/local/bin/anvilmq"]
