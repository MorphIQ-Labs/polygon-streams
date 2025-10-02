# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs`: Tokio app that connects to Polygon.io WebSocket and prints messages to stdout.
- `src/config.rs`: Centralized configuration loaded from environment (API key, topic, URL, channel size, backoff).
- `src/model.rs`: Serde models for Polygon messages plus unit tests.
- Binaries: crate name `polygon-rs` builds to `target/release/polygon-rs`.
- CI: `.gitlab-ci.yml` runs `fmt`, `clippy`, and tests on each change. Docker image is built and pushed.

## Build, Test, and Development Commands
- Build: `cargo build` (use `--release` for production).
- Run (env required):
  - `export POLYGON_API_KEY=...`
  - Optionally set topic: `export subscription='T.*'` (e.g., `Q.*`, `T:ES*`)
  - Optional tuning: `POLYGON_WS_URL`, `CHANNEL_CAP`, `BACKOFF_INITIAL_SECS`, `BACKOFF_MAX_SECS`, `READ_TIMEOUT_SECS`, `HEARTBEAT_INTERVAL_SECS`, `STATS_INTERVAL_SECS`, `BP_WARN_INTERVAL_SECS`, `EMIT_NDJSON`, `NDJSON_DEST`, `NDJSON_SPLIT_PER_TYPE`
  - `RUST_LOG=info cargo run --release -- [flags]`  # flags override env
    - Flags: `--subscription`, `--ws-url`, `--channel-cap`, `--backoff-initial-secs`, `--backoff-max-secs`, `--read-timeout-secs`, `--heartbeat-interval-secs`, `--stats-interval-secs`
- Test: `cargo test` (unit tests live alongside code).
- Lint/format: `cargo fmt -- --check` and `cargo clippy -- -D warnings` (matches CI).
- Docker: `docker build -t polygon-rs .` then `docker run --rm -e POLYGON_API_KEY=$POLYGON_API_KEY polygon-rs`.

## Coding Style & Naming Conventions
- Rust 2018 edition with `rustfmt` defaults; 4‑space indentation.
- Use `snake_case` for functions/modules, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Prefer `Result` error propagation; reserve `panic!` for unrecoverable states.
- Logging via `tracing` with JSON output to stdout; control verbosity with `RUST_LOG` (e.g., `info`, `debug`).

## Testing Guidelines
- Framework: Rust built‑in tests (`#[cfg(test)]`) in `src/*.rs`.
- Add integration tests under `tests/` when behavior spans modules or I/O; mock network where possible.
- Aim to cover parsing/serialization in `model` and critical paths in `main` helpers.
- No coverage threshold enforced; optional local coverage via `cargo tarpaulin`.

## Commit & Pull Request Guidelines
- Commit style in history is short, imperative messages (e.g., "fix ci/cd", "add README"). Keep subjects concise; include a scope when helpful (e.g., `ci:`, `docs:`).
- PRs should include: summary, motivation, linked issues, testing notes, and a run example (env + `cargo run` or Docker). Include screenshots/log snippets when relevant.

## Security & Configuration Tips
- Never commit secrets; configure via environment variables (`POLYGON_API_KEY`).
- When sharing examples, sanitize keys and endpoints.
- For Docker/K8s, pass env vars via secrets; no ports are required since output is stdout.
