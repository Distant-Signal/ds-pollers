# syntax=docker/dockerfile:1
# Multi-stage build for the `poller-ldbws` service.
#
# Builder pin: matches poller-incidents/poller-stations/poller-tocs at
# rust:1.86-bookworm — this crate pulls in the same reqwest -> idna/icu_*
# transitive chain requiring rustc 1.86+.
#
# Build from the repo root so the workspace's Cargo.toml/Cargo.lock and
# crates/common path dependency are all in the build context:
#   docker build -f docker/poller-ldbws.Dockerfile .
#
# CARGO_PROFILE picks the cargo build profile (and matching target/<profile>
# output dir): "release" (default) for optimized builds, "debug" for fast
# unoptimized dev builds. Set to "debug" by docker-compose.dev.yml, the
# override that `dev.env` selects via COMPOSE_FILE; docker-compose.yml on
# its own leaves it at "release".
ARG CARGO_PROFILE=release

FROM rust:1.86-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
# BuildKit cache mounts: the cargo registry, the git checkouts and the
# target/ dir all live in caches that persist across builds, so a rebuild
# recompiles only what actually changed instead of the whole dependency
# tree. Requires the `# syntax=` directive at the top of this file.
#
# The target cache id is keyed by rustc version (`cargo-target-1.86`) rather
# than shared across all seven Rust services. Cargo's fingerprints include the
# compiler version, so the 1.88 services (api, aggregator, enricher) and the
# 1.86 ones (the four pollers) would otherwise invalidate and fully recompile
# each other's artifacts on every alternating build. The registry and git
# caches hold only downloaded sources, so sharing those across all seven is
# safe.
#
# `sharing=locked` because docker-compose builds services in parallel, and
# concurrent cargo invocations must not share one target dir unserialised.
#
# The trailing `cp` is the non-obvious part: a cache mount is NOT part of the
# resulting image layer, so /app/target ceases to exist the moment this RUN
# finishes and a later `COPY --from=builder /app/target/...` would find
# nothing. The binary has to be copied out to a normal path within the same
# RUN — which is why the runtime stage below copies from /usr/local/bin/poller-ldbws.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.86,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin poller-ldbws; \
    else \
      cargo build --bin poller-ldbws; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/poller-ldbws /usr/local/bin/poller-ldbws

FROM debian:bookworm-slim

# reqwest's native-tls backend verifies certs against the system store, so
# the runtime image needs a CA bundle even though it otherwise only carries
# the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin poller

COPY --from=builder /usr/local/bin/poller-ldbws /usr/local/bin/poller-ldbws

USER poller

ENTRYPOINT ["/usr/local/bin/poller-ldbws"]
