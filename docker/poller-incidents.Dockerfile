# Multi-stage build for the `poller-incidents` service.
#
# Builder pin: edition 2024 (used by every crate in this workspace) needs
# rustc 1.85+, but this crate's resolved Cargo.lock also pulls in transitive
# deps (icu_* via idna/url, used by reqwest) whose *published* crate version
# requires rustc 1.86+. 1.86 is the actual floor for this lockfile, not 1.85.
#
# Build from the repo root so the workspace's `Cargo.toml`/`Cargo.lock` and
# `crates/common` path dependency are all in the build context:
#   docker build -f docker/poller-incidents.Dockerfile .
FROM rust:1.86-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin poller-incidents

FROM debian:bookworm-slim

# reqwest's native-tls backend verifies certs against the system store, so
# the runtime image needs a CA bundle even though it otherwise only carries
# the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin poller

COPY --from=builder /app/target/release/poller-incidents /usr/local/bin/poller-incidents

USER poller

ENTRYPOINT ["/usr/local/bin/poller-incidents"]
