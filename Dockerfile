# Stage 1: Chef — dependency caching layer
FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Planner — compute recipe from lock file
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder — build dependencies first (cached), then source
FROM chef AS builder
RUN apt-get update && apt-get install -y cmake g++ && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin strata-server

# Stage 4: Runtime — minimal image
FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=builder /app/target/release/strata-server /usr/local/bin/strata-server
EXPOSE 5432 8432 9432
VOLUME ["/data"]
ENV STRATA_STORAGE__DATA_DIR=/data
ENTRYPOINT ["strata-server"]
