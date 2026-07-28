# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p falcon-cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10001 falcon

COPY --from=builder /build/target/release/falcon /usr/local/bin/falcon

USER falcon
WORKDIR /home/falcon
# No VOLUME: the cache is pure RAM and writes nothing to disk.

# 8080 = HTTP/UI, 6380 = binary wire protocol
EXPOSE 8080 6380

# Falcon is configured via the CLI (never env vars). Bake a default profile into
# the image, then serve from it. Override the settings by mounting your own
# profile at ~/.falcon or by running
# `docker run ... falcon-image config set <key> <value>` before `serve`.
ENV HOME=/home/falcon
# Capacity is deliberately NOT set: the cache reads the container's own memory
# limit at startup and sizes itself to it, so `docker run -m 2g` is all the
# configuration it needs.
RUN falcon config set http-bind 0.0.0.0:8080 && \
    falcon config set wire-bind 0.0.0.0:6380 && \
    falcon config set wire-enabled true

# Liveness uses /healthz (always 200 while the process is up). Orchestrators
# should use /readyz for readiness (503 until startup completes) and scrape
# /metrics for autoscaling signals.
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

# SIGTERM triggers a graceful drain; give it time to finish. The cache holds
# nothing durable, so there is no final flush to wait on — but in-flight
# requests still deserve to complete.
STOPSIGNAL SIGTERM

# `serve` runs the node from the baked-in profile. To reconfigure, run the CLI:
#   docker run ... falcon-image config set http-bind 0.0.0.0:9090
# then serve. Serve flags can also override for one run: falcon serve --region ...
ENTRYPOINT ["falcon"]
CMD ["serve"]
