FROM rust:latest AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY static ./static/
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ircstore /usr/local/bin/ircstore
COPY --from=builder /app/target/release/ircstore-web /usr/local/bin/ircstore-web
COPY static /app/static
WORKDIR /app
