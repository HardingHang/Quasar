# Stage 1: Build
FROM rust:1.86-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p quasar-server

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/quasar-server /usr/local/bin/quasar-server
EXPOSE 8080
ENV QUASAR_HOST=0.0.0.0
ENV QUASAR_PORT=8080
CMD ["quasar-server"]
