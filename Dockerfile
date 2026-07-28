# Stage 1: build
FROM rust:latest-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock* ./
COPY crates/ ./crates/
RUN cargo build --release

# Stage 2: runtime — distroless, usuário não-root, read-only fs
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /app/target/release/bridge-api /app/
COPY --from=builder /app/target/release/bridge-worker /app/
COPY --from=builder /app/target/release/bridge-scheduler /app/
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
# ponytail: filesystem read-only, sem shell — tudo pelo docker-compose
