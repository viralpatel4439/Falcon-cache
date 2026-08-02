# syntax=docker/dockerfile:1

# ---- builder ---------------------------------------------------------------
# Pinned to match rust-toolchain.toml. A floating `rust:1-bookworm` would let
# the image drift to a different compiler than local builds and CI use.
FROM rust:1.97-bookworm AS builder

WORKDIR /build

# Dependency layer. Copying only the manifests and building against stub
# sources means this layer is cached on the Cargo.toml/Cargo.lock hash alone —
# editing application code no longer rebuilds every dependency from scratch,
# which is the difference between a ~10s and a ~3min incremental image build.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/falcon-storage/Cargo.toml crates/falcon-storage/
COPY crates/falcon-core/Cargo.toml    crates/falcon-core/
COPY crates/falcon-api/Cargo.toml     crates/falcon-api/
COPY crates/falcon-wire/Cargo.toml    crates/falcon-wire/
COPY crates/falcon-cli/Cargo.toml     crates/falcon-cli/
RUN mkdir -p crates/falcon-storage/src crates/falcon-core/src crates/falcon-api/src \
             crates/falcon-wire/src crates/falcon-cli/src \
    && echo '' > crates/falcon-storage/src/lib.rs \
    && echo '' > crates/falcon-core/src/lib.rs \
    && echo '' > crates/falcon-api/src/lib.rs \
    && echo '' > crates/falcon-wire/src/lib.rs \
    && echo 'fn main() {}' > crates/falcon-cli/src/main.rs \
    && cargo build --release -p falcon-cli \
    # Drop every artifact built from the stubs — the .rlibs included. Leaving
    # them behind makes cargo link the real binary against empty libraries;
    # deleting only the binary is not enough.
    && rm -rf crates \
       target/release/falcon \
       target/release/deps/falcon* \
       target/release/deps/libfalcon* \
       target/release/.fingerprint/falcon*

# Application layer: rebuilt whenever sources change, dependencies reused.
COPY crates ./crates
RUN cargo build --release -p falcon-cli

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10001 falcon

COPY --from=builder /build/target/release/falcon /usr/local/bin/falcon

USER falcon
WORKDIR /home/falcon
# No VOLUME: the cache is pure RAM and writes nothing to disk.

# 8080 = HTTP/REST, 6380 = binary wire protocol
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
# should use /readyz for readiness (503 until startup completes). There is no
# /metrics endpoint; /health returns cache statistics as JSON.
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
