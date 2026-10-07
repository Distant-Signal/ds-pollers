# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Multi-stage build for the `poller-nir-stations` service.
#
# Same rustc 1.88 floor as every other crate in this workspace -- see
# docker/poller-stations.Dockerfile's own comment for the confirmed
# icu_provider transitive-dependency reasoning.
#
# Build from the repo root:
#   docker build -f docker/poller-nir-stations.Dockerfile .
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "${CARGO_PROFILE}" = "release" ]; then \
      cargo build --release --bin poller-nir-stations; \
    else \
      cargo build --bin poller-nir-stations; \
    fi \
    && cp "/app/target/${CARGO_PROFILE}/poller-nir-stations" /usr/local/bin/poller-nir-stations

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

# tini as PID 1 (signal forwarding, zombie reaping): see
# docker/api.Dockerfile's runtime stage for why, and for the version pin.
ARG TINI_VERSION=0.19.0-1

# hadolint ignore=DL3008 # apt versions unpinned on purpose; see .hadolint.yaml
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates "tini=${TINI_VERSION}*" \
    && rm -rf /var/lib/apt/lists/* \
    && tini --version \
    && groupadd --system --gid 1000 poller \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 poller

COPY --from=builder /usr/local/bin/poller-nir-stations /usr/local/bin/poller-nir-stations

USER 1000:1000

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/poller-nir-stations"]
