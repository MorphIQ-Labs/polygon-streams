# Build stage
FROM rust:1.90-bookworm AS build
WORKDIR /usr/src/polygon-rs
COPY . .

# Build the project in release mode tuned for small size via Cargo profile env overrides
# These avoid RUSTFLAGS conflicts (e.g., embed-bitcode vs LTO) and are stable
ENV CARGO_PROFILE_RELEASE_LTO=true \
    CARGO_PROFILE_RELEASE_OPT_LEVEL=z \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_STRIP=symbols
ARG ENABLE_ZMQ_SINK=0
ARG ENABLE_NNG_SINK=0
RUN apt-get update && \
    if [ "$ENABLE_ZMQ_SINK" = "1" ]; then apt-get install -y --no-install-recommends libzmq3-dev cmake; fi && \
    if [ "$ENABLE_NNG_SINK" = "1" ]; then apt-get install -y --no-install-recommends libnng-dev cmake; fi && \
    rm -rf /var/lib/apt/lists/* && \
    FEATURES=""; \
    if [ "$ENABLE_ZMQ_SINK" = "1" ] && [ "$ENABLE_NNG_SINK" = "1" ]; then \
      FEATURES="zmq-sink,nng-sink"; \
    elif [ "$ENABLE_ZMQ_SINK" = "1" ]; then \
      FEATURES="zmq-sink"; \
    elif [ "$ENABLE_NNG_SINK" = "1" ]; then \
      FEATURES="nng-sink"; \
    fi; \
    if [ -n "$FEATURES" ]; then \
      cargo build --release --features "$FEATURES"; \
    else \
      cargo build --release; \
    fi

# Final runtime stage
FROM debian:bookworm-slim AS runtime
# Install required runtime dependencies (OpenSSL runtime only)
ARG ENABLE_ZMQ_SINK=0
ARG ENABLE_NNG_SINK=0
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    $(if [ "$ENABLE_ZMQ_SINK" = "1" ]; then echo libzmq5; fi) \
    $(if [ "$ENABLE_NNG_SINK" = "1" ]; then echo libnng1; fi) \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary
COPY --from=build /usr/src/polygon-rs/target/release/polygon-rs /usr/local/bin/polygon-rs

# Set the entrypoint to polygon-rs
ENTRYPOINT ["/usr/local/bin/polygon-rs"]

# Default arguments (optional, empty here)
CMD []
