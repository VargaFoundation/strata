# Stage 1: Chef — dependency caching layer
FROM rust:1-alpine AS chef
RUN apk add --no-cache musl-dev cmake g++ make pkgconf openssl-dev openssl-libs-static perl
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Planner — compute recipe from lock file
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder — build dependencies first (cached), then source
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin ecphoria-server && \
    strip target/release/ecphoria-server

# Stage 4: Runtime — minimal scratch-like image
FROM alpine:3.21 AS runtime
RUN apk add --no-cache ca-certificates curl && \
    adduser -D -u 1000 ecphoria
COPY --from=builder /app/target/release/ecphoria-server /usr/local/bin/ecphoria-server

USER ecphoria
EXPOSE 5432 8432 9432 9433
VOLUME ["/data"]

ENV ECPHORIA_STORAGE__DATA_DIR=/data
ENV ECPHORIA_MEMORY__EPISODIC__DB_PATH=/data/episodic.duckdb
ENV ECPHORIA_MEMORY__STATE__DB_PATH=/data/state.db
ENV ECPHORIA_MEMORY__SEMANTIC__INDEX_DIR=/data/vectors

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s \
    CMD curl -f http://localhost:8432/health || exit 1

ENTRYPOINT ["ecphoria-server"]
