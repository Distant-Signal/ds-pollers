# Multi-stage build for the `poller-ldbws` service.
#
# Builder pin: matches poller-incidents/poller-stations/poller-tocs at
# rust:1.86-bookworm — this crate pulls in the same reqwest -> idna/icu_*
# transitive chain requiring rustc 1.86+.
#
# Build from the repo root so the workspace's Cargo.toml/Cargo.lock and
# crates/common path dependency are all in the build context:
#   docker build -f docker/poller-ldbws.Dockerfile .
FROM rust:1.86-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin poller-ldbws

FROM debian:bookworm-slim

# reqwest's native-tls backend verifies certs against the system store, so
# the runtime image needs a CA bundle even though it otherwise only carries
# the one binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin poller

COPY --from=builder /app/target/release/poller-ldbws /usr/local/bin/poller-ldbws

USER poller

ENTRYPOINT ["/usr/local/bin/poller-ldbws"]
