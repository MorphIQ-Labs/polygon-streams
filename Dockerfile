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
RUN cargo build --release

# Final runtime stage
FROM debian:bookworm-slim AS runtime
# Install required runtime dependencies (OpenSSL runtime only)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary
COPY --from=build /usr/src/polygon-rs/target/release/polygon-rs /usr/local/bin/polygon-rs

# Set the entrypoint to polygon-rs
ENTRYPOINT ["/usr/local/bin/polygon-rs"]

# Default arguments (optional, empty here)
CMD []
