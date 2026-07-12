# Multi-stage build for the `poller-tocs` service.
#
# Builder pin: edition 2024 (used by every crate in this workspace) needs
# rustc 1.85+, but this crate's resolved Cargo.lock also pulls in transitive
# deps (icu_* via idna/url, used by reqwest) whose *published* crate version
# requires rustc 1.86+. 1.86 is the actual floor for this lockfile, not 1.85.
#
# Build from the repo root so the workspace's `Cargo.toml`/`Cargo.lock` and
# `crates/common` path dependency are all in the build context:
#   docker build -f docker/poller-tocs.Dockerfile .
#
# CARGO_PROFILE picks the cargo build profile (and matching target/<profile>
# output dir): "release" (default) for optimized builds, "debug" for fast
# unoptimized dev builds. Set via docker-compose's `--profile dev` (see
# docker-compose.yml).
ARG CARGO_PROFILE=release

FROM rust:1.86-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin poller-tocs; \
    else \
      cargo build --bin poller-tocs; \
    fi

FROM debian:bookworm-slim
ARG CARGO_PROFILE

# reqwest's native-tls backend verifies certs against the system store, so
# the runtime image needs a CA bundle even though it otherwise only carries
# the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin poller

COPY --from=builder /app/target/${CARGO_PROFILE}/poller-tocs /usr/local/bin/poller-tocs

USER poller

ENTRYPOINT ["/usr/local/bin/poller-tocs"]
