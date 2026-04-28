# Build stage — Alpine + musl for static binary
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev gcc make pkgconf openssl-dev openssl-libs-static
ENV OPENSSL_STATIC=1 OPENSSL_DIR=/usr
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

# Runtime stage — minimal Alpine
FROM alpine:3
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/moo /usr/local/bin/
EXPOSE 3307
ENTRYPOINT ["moo"]
CMD ["start", "--listen=0.0.0.0:3307"]
