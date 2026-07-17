FROM rust:slim-bookworm as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /usr/local/bin
RUN apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/target/release/tendermint-sim .
COPY start.sh .
RUN chmod +x start.sh
CMD ["./start.sh"]
