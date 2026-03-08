# ── Build stage ──────────────────────────────────────────────────────────────
FROM rust:1.85-slim AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/

RUN cargo build --release --features "redis,cluster"

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN groupadd --system fluxdb && \
    useradd --system --gid fluxdb --create-home --home-dir /var/lib/fluxdb fluxdb

COPY --from=builder /build/target/release/fluxdb /usr/local/bin/fluxdb

RUN mkdir -p /etc/fluxdb /var/lib/fluxdb && \
    chown fluxdb:fluxdb /var/lib/fluxdb

USER fluxdb
WORKDIR /var/lib/fluxdb

EXPOSE 7654 6379 7655

VOLUME ["/var/lib/fluxdb"]

ENTRYPOINT ["fluxdb"]
CMD ["--listen", "0.0.0.0:7654", "--data-dir", "/var/lib/fluxdb"]
