# syntax=docker/dockerfile:1
# Multi-stage build for the `poller-incidents` service.
#
# Builder pin: edition 2024 (used by every crate in this workspace) needs
# rustc 1.85+, but this crate's resolved Cargo.lock also pulls in transitive
# icu_* crates (via reqwest -> url -> idna -> idna_adapter ->
# icu_normalizer/icu_provider, through the `common` crate every poller
# depends on) that require rustc 1.88+ -- confirmed by actually building
# this image against rust:1.86-bookworm first and hitting:
#   "error: rustc 1.86.0 is not supported ... icu_provider@2.3.1 requires
#   rustc 1.88"
# 1.88 is the actual floor for this lockfile, not 1.86 or 1.85.
#
# Build from the repo root so the workspace's `Cargo.toml`/`Cargo.lock` and
# `crates/common` path dependency are all in the build context:
#   docker build -f docker/poller-incidents.Dockerfile .
#
# CARGO_PROFILE picks the cargo build profile (and matching target/<profile>
# output dir): "release" (default) for optimized builds, "debug" for fast
# unoptimized dev builds. Set to "debug" by docker-compose.dev.yml, the
# override that `dev.env` selects via COMPOSE_FILE; docker-compose.yml on
# its own leaves it at "release".
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
# BuildKit cache mounts: the cargo registry, the git checkouts and the
# target/ dir all live in caches that persist across builds, so a rebuild
# recompiles only what actually changed instead of the whole dependency
# tree. Requires the `# syntax=` directive at the top of this file.
#
# The target cache id is keyed by rustc version (`cargo-target-1.88`). Every
# Rust service in this workspace now builds with the same rustc version, so
# this id is shared across all of them -- see docker-compose.yml's top-of-file
# comment for the full list. The registry and git caches hold only downloaded
# sources, so sharing those across all of them is safe too.
#
# `sharing=locked` because docker-compose builds services in parallel, and
# concurrent cargo invocations must not share one target dir unserialised.
#
# The trailing `cp` is the non-obvious part: a cache mount is NOT part of the
# resulting image layer, so /app/target ceases to exist the moment this RUN
# finishes and a later `COPY --from=builder /app/target/...` would find
# nothing. The binary has to be copied out to a normal path within the same
# RUN — which is why the runtime stage below copies from /usr/local/bin/poller-incidents.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin poller-incidents; \
    else \
      cargo build --bin poller-incidents; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/poller-incidents /usr/local/bin/poller-incidents

FROM debian:bookworm-slim

# reqwest's native-tls backend verifies certs against the system store, so
# the runtime image needs a CA bundle even though it otherwise only carries
# the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 poller \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 poller

COPY --from=builder /usr/local/bin/poller-incidents /usr/local/bin/poller-incidents

# Numeric USER, not the `poller` name useradd created above: Kubernetes'
# runAsNonRoot admission check (this chart's podSecurityContext sets
# runAsNonRoot: true with no explicit runAsUser) resolves the image's
# config purely from its manifest -- it does NOT read /etc/passwd inside
# the image -- so a symbolic USER fails admission with "container has
# runAsNonRoot and image has non-numeric user, cannot verify user is
# non-root". Pinned to the same uid/gid useradd was given above so this
# stays in sync with the group ownership set via COPY --chown/groupadd.
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/poller-incidents"]
