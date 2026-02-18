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
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

# REST API port (see src/api/rest/mod.rs)
EXPOSE 3000

ENTRYPOINT ["./docker-entrypoint.sh"]
