# GaussFlow — multi-stage build.
#
# Stage 1 builds the workspace in release mode (protoc is required by the gRPC build scripts).
# Stage 2 is a slim runtime image containing just the web server + CLI and the web static assets.

FROM rust:1.83-slim AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
# Build only what we ship (the web server and the CLI).
RUN cargo build --release -p gaussflow-web -p gaussflow-cli

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/gaussflow-web /usr/local/bin/gaussflow-web
COPY --from=builder /app/target/release/gaussflow-cli /usr/local/bin/gaussflow
# Web static assets are served relative to the working directory.
COPY --from=builder /app/gaussflow-web/static /app/gaussflow-web/static

# Runs with no database by default (in-memory run store). Set GAUSSFLOW_RUN_STORE=surreal and the
# GAUSSFLOW_DB_* vars to use SurrealDB; set GAUSSFLOW_JWT_SECRET to enable the authenticated API.
ENV RUST_LOG=info
EXPOSE 8080
ENTRYPOINT ["gaussflow-web"]
