FROM rust:1.94
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release
COPY config /app/config
EXPOSE 8787
ENV IPTV_CONFIG=/app/config/sources.yaml
CMD ["/app/target/release/home-iptv-proxy"]
