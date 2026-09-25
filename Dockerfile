FROM rust:1.94-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/home-iptv-proxy /app/home-iptv-proxy
EXPOSE 28788
ENV IPTV_CONFIG=/app/config/sources.yaml
CMD ["/app/home-iptv-proxy"]
