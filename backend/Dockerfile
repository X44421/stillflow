FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN useradd --create-home --uid 10001 stillflow \
    && mkdir -p /data \
    && chown stillflow:stillflow /data
COPY --from=builder /app/target/release/stillflow-backend /usr/local/bin/stillflow-backend

ENV STILLFLOW_BIND=0.0.0.0:8787
ENV STILLFLOW_DATA_DIR=/data

VOLUME ["/data"]
EXPOSE 8787
USER stillflow

CMD ["stillflow-backend"]
