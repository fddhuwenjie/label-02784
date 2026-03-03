### Stage 1: Build
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Cache dependencies
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

# Build application
COPY src/ src/
RUN touch src/main.rs && cargo build --release

### Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/media-prompt-service /app/media-prompt-service
COPY config/ /app/config/

ENV APP_CONFIG_PATH=/app/config/config.yaml
ENV RUST_LOG=info

EXPOSE 9999

CMD ["/app/media-prompt-service"]
