# ── Build stage ──────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

# install SSL and LZ4
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency compilation separately from source changes.
# Dummy entry-points are created so cargo can resolve and compile all deps
# without the real source being present yet.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --bin anysignal \
    && rm -rf src

# Build the real binary.
COPY . .
RUN cargo build --release --bin anysignal


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# install SSL and LZ4
RUN apt-get update && apt-get install -y \
    ca-certificates \
    liblz4-1 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/anysignal ./

# REST API port (see src/api/rest/mod.rs)
EXPOSE 3000

CMD ["/app/anysignal"]
