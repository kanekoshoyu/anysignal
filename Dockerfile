# ── Build stage ──────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependency compilation separately from source changes.
# Dummy entry-points are created so cargo can resolve and compile all deps
# without the real source being present yet.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn main() {}" > src/bin/load_schema.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary.
COPY . .
# Touch main.rs so cargo notices the source changed after the dummy build.
RUN touch src/main.rs src/bin/load_schema.rs \
    && cargo build --release --bin anysignal

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    liblz4-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/anysignal ./

# All configuration via env vars — set these in Coolify UI
ENV RUNNERS=${RUNNERS}
ENV API_KEY_COINMARKETCAP=${API_KEY_COINMARKETCAP}
ENV API_KEY_NEWSAPI=${API_KEY_NEWSAPI}
ENV API_KEY_YOUTUBE_DATA_V3=${API_KEY_YOUTUBE_DATA_V3}
ENV API_KEY_POLYGONIO=${API_KEY_POLYGONIO}
ENV API_KEY_ALPHAVANTAGE=${API_KEY_ALPHAVANTAGE}
ENV API_KEY_USTREASURY=${API_KEY_USTREASURY}
ENV API_KEY_SECAPI=${API_KEY_SECAPI}
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV AWS_REGION=${AWS_REGION}
ENV HYPERLIQUID_S3_BUCKET=${HYPERLIQUID_S3_BUCKET}
ENV QUESTDB_ADDR=${QUESTDB_ADDR}
ENV QUESTDB_USER=${QUESTDB_USER}
ENV QUESTDB_PASSWORD=${QUESTDB_PASSWORD}
ENV RUST_LOG=${RUST_LOG}

# REST API port (see src/api/rest/mod.rs)
EXPOSE 3000

CMD ["/app/anysignal"]
