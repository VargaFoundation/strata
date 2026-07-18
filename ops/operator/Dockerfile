# Multi-stage build for the Ecphoria sharding operator (a standalone crate, not part of the workspace).
# Build context is this directory: `docker build -t ghcr.io/vargafoundation/ecphoria-operator ops/operator`.
FROM rust:1-alpine AS chef
# aws-lc-sys (via reqwest/kube rustls) needs cmake + a C/C++ toolchain + perl; openssl-dev covers
# kube's default TLS backend if rustls isn't selected.
RUN apk add --no-cache musl-dev cmake g++ make pkgconf perl openssl-dev openssl-libs-static
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin ecphoria-operator && strip target/release/ecphoria-operator

FROM alpine:3.21 AS runtime
RUN apk add --no-cache ca-certificates && adduser -D -u 1000 operator
COPY --from=builder /app/target/release/ecphoria-operator /usr/local/bin/ecphoria-operator
USER operator
ENTRYPOINT ["ecphoria-operator"]
