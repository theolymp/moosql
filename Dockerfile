# Build stage
FROM rust:1.82-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/mariadb-cow /usr/local/bin/
EXPOSE 3307
ENTRYPOINT ["mariadb-cow"]
CMD ["start", "--listen=0.0.0.0:3307"]
