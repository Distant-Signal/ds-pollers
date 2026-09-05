# syntax=docker/dockerfile:1
# Multi-stage build for the `poller-irish-rail-gtfs` service.
#
# Same rustc 1.88 floor as every other crate in this workspace -- see
# docker/poller-stations.Dockerfile's own comment for the confirmed
# icu_provider transitive-dependency reasoning.
#
# Build from the repo root:
#   docker build -f docker/poller-irish-rail-gtfs.Dockerfile .
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin poller-irish-rail-gtfs; \
    else \
      cargo build --bin poller-irish-rail-gtfs; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/poller-irish-rail-gtfs /usr/local/bin/poller-irish-rail-gtfs

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 poller \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 poller

COPY --from=builder /usr/local/bin/poller-irish-rail-gtfs /usr/local/bin/poller-irish-rail-gtfs

USER 1000:1000

ENTRYPOINT ["/usr/local/bin/poller-irish-rail-gtfs"]
