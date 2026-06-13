FROM rust:1.94 AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/home-iptv-proxy /usr/local/bin/home-iptv-proxy
COPY config /app/config
EXPOSE 8787
ENV IPTV_CONFIG=/app/config/sources.yaml
CMD ["home-iptv-proxy"]
