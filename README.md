# Polygon.io Multi‑Cluster WebSocket Streamer (Rust)

High‑performance streamer for Polygon.io clusters (stocks, futures, options, crypto). Normalizes and logs events, with optional NDJSON for ETL, plus health/metrics for ops.

## Cheat Sheet (Env Vars & Defaults)

| Variable | Default | Notes |
| --- | --- | --- |
| `POLYGON_API_KEY` | — (required) | Your Polygon.io API key |
| `POLYGON_CLUSTER` | `futures` | One of: `stocks`, `futures`, `options`, `crypto` |
| `SUBSCRIPTION` | `T.*` | Topic pattern (e.g., `Q.*`, `T:ES*`) |
| `CHANNEL_CAP` | `1024` | Inbound channel capacity |
| `BACKOFF_INITIAL_SECS` | `1` | Reconnect backoff initial seconds |
| `BACKOFF_MAX_SECS` | `60` | Reconnect backoff max seconds |
| `READ_TIMEOUT_SECS` | `60` | Read timeout before reconnect |
| `HEARTBEAT_INTERVAL_SECS` | `30` | `0` disables heartbeats |
| `METRICS_ADDR` | `127.0.0.1:9898` | Exposes `/metrics`, `/health`, `/ready` |
| `STATS_INTERVAL_SECS` | `60` | Periodic stats log interval |
| `BP_WARN_INTERVAL_SECS` | `5` | Backpressure warning interval |
| `SINK` | `stdout` | One of: `stdout`, `ndjson`, `zmq` |
| `EMIT_NDJSON` | `false` | Legacy toggle; if `SINK` unset and this is `true`, selects `ndjson` |
| `NDJSON_DEST` | `stdout` | `stdout` or file path |
| `NDJSON_CHANNEL_CAP` | `2048` | NDJSON pipeline channel capacity |
| `NDJSON_WARN_INTERVAL_SECS` | `5` | NDJSON backpressure warn interval |
| `NDJSON_FLUSH_EVERY` | `100` | Flush after N events |
| `NDJSON_FLUSH_INTERVAL_MS` | `1000` | Max flush interval in ms |
| `NDJSON_MAX_BYTES` | disabled | Rotate when size reached (disabled if unset) |
| `NDJSON_ROTATE_SECS` | disabled | Time-based rotation in seconds (disabled if unset) |
| `NDJSON_GZIP` | `false` | Gzip rotated files |
| `NDJSON_NO_REOPEN` | `false` | When `true`, disable reopen on SIGHUP |
| `NDJSON_INCLUDE` | none | Comma patterns (e.g., `T:ES*,Q:ES*`) |
| `NDJSON_SAMPLE_QUOTES` | `1` | Emit 1 in N quotes |
| `NDJSON_SPLIT_PER_TYPE` | `false` | Split NDJSON files by type |
| `ZMQ_ENDPOINT` | `tcp://127.0.0.1:5556` | ZMQ PUB endpoint (connect or bind) |
| `ZMQ_BIND` | `false` | When `true`, bind instead of connect |
| `ZMQ_CHANNEL_CAP` | `4096` | Internal channel capacity for ZMQ sink |
| `ZMQ_WARN_INTERVAL_SECS` | `5` | Backpressure warn interval for ZMQ |
| `ZMQ_SND_HWM` | unset | Optional zmq `SNDHWM` (messages) |
| `ZMQ_TOPIC_PREFIX` | empty | Optional topic prefix for PUB topic |
| `READY_MIN_MESSAGES` | `1` | Messages required before `/ready` returns READY |
| `READY_DELAY_MS` | `0` | Additional delay before READY (milliseconds) |

## Features

- Real-time streaming of stock market data
- Support for multiple message types:
    - Trades
    - Quotes
    - Second-level aggregates
    - Minute-level aggregates
- Automatic reconnection and error handling
- Configurable logging levels
- Graceful shutdown on Ctrl+C

## Prerequisites

- Rust 1.70 or higher
- Valid Polygon.io API key

## Installation

1. Clone and build:
   ```bash
   git clone [repository-url]
   cd polygon-streams
   cargo build --release
   # ZMQ sink requires libzmq and feature flag:
   # macOS (brew):   brew install zeromq
   # Debian/Ubuntu:  sudo apt-get install -y libzmq3-dev
   # RHEL/CentOS:    sudo yum install -y zeromq-devel
   # Then build with: cargo build --release --features zmq-sink
   ```

## Configuration

Set required environment variables:
```bash
export POLYGON_API_KEY="your_api_key_here"
# Optional: override subscription topic (default is 'T.*')
# export SUBSCRIPTION="T:ES*"   # examples: 'T.*', 'Q.*', 'T:ES*'
# Select a cluster to derive the WebSocket URL automatically:
# export POLYGON_CLUSTER=futures   # options: stocks | futures | options | crypto (default: futures)
# Mappings:
#   stocks  -> wss://socket.polygon.io/stocks
#   futures -> wss://socket.polygon.io/futures
#   options -> wss://socket.polygon.io/options
#   crypto  -> wss://socket.polygon.io/crypto
# Optional: channel capacity, reconnect backoff, read timeout, heartbeat, metrics/health
# export CHANNEL_CAP=1024             # default: 1024
# export BACKOFF_INITIAL_SECS=1       # default: 1
# export BACKOFF_MAX_SECS=60          # default: 60
# export READ_TIMEOUT_SECS=60         # default: 60
# export HEARTBEAT_INTERVAL_SECS=30   # default: 30 (0 to disable)
# export METRICS_ADDR='127.0.0.1:9898'  # serves /metrics and /health (default: 127.0.0.1:9898)
# Stats logging interval:
# export STATS_INTERVAL_SECS=60       # default: 60
# Readiness (affects /ready):
# export READY_MIN_MESSAGES=1         # default: 1
# export READY_DELAY_MS=0             # default: 0 (milliseconds)
# NDJSON event stream (always emitted if enabled):
# export EMIT_NDJSON=true              # default: false
# export NDJSON_DEST=stdout            # or file path (default: stdout)
# export NDJSON_CHANNEL_CAP=2048       # default: 2048
# export NDJSON_WARN_INTERVAL_SECS=5   # default: 5
# conservative rotation/flush options (disabled unless set):
# export NDJSON_MAX_BYTES=104857600   # 100MB (unset = disabled)
# export NDJSON_ROTATE_SECS=3600      # unset = disabled
# export NDJSON_GZIP=false             # default: false
# export NDJSON_FLUSH_EVERY=100        # default: 100
# export NDJSON_FLUSH_INTERVAL_MS=1000 # default: 1000
# export NDJSON_NO_REOPEN=false        # default: false; reopen on SIGHUP enabled by default
# Backpressure warn interval (seconds):
# export BP_WARN_INTERVAL_SECS=5       # default: 5
```

## Usage

Run the application (env‑only configuration):
```bash
RUST_LOG=info cargo run --release
```

Examples
- Stdout sink (default) with stocks cluster:
  - `export POLYGON_CLUSTER=stocks && SINK=stdout RUST_LOG=warn,polygon_events=info cargo run --release`
- NDJSON to file with futures cluster:
  - `export POLYGON_CLUSTER=futures && SINK=ndjson NDJSON_DEST=/var/log/polygon.ndjson RUST_LOG=warn cargo run --release`
- NDJSON to stdout (pipe to jq):
  - `SINK=ndjson NDJSON_DEST=stdout RUST_LOG=warn cargo run --release | jq -c .`
- ZMQ PUB sink (requires feature build):
  - `cargo run --release --features zmq-sink` with `SINK=zmq ZMQ_ENDPOINT=tcp://127.0.0.1:5556`
  - Subscriber examples:
    - Rust: `cargo run --release --features zmq-sink --example zmq_sub`
    - Python: `pip install pyzmq && ZMQ_SUB_ENDPOINT=tcp://127.0.0.1:5556 python3 examples/zmq_sub.py`

## Output Format

Messages are logged at `info` as structured JSON to stdout. Each record includes a `line` field with the normalized prefix format for grep-friendly use:
- Futures Trades: `T:{symbol} {json_data}` (in `fields.line`)
- Futures Quotes: `Q:{symbol} {json_data}` (in `fields.line`)

## Example Consumption

Override topic via env and filter AAPL trades (enable info logs):
```bash
export SUBSCRIPTION='T.*'
RUST_LOG=info cargo run --release | rg '"line":"T:AAPL'
```

Log filtering tips
- Enable only event logs at info with everything else at warn:
  - `RUST_LOG=warn,polygon_events=info`
- Include periodic stats as well:
  - `RUST_LOG=warn,polygon_events=info,polygon_stats=info`

NDJSON stream
- Enable with `SINK=ndjson`.
- Each event is emitted as one JSON line with fields: `ingest_ts`, `type`, `symbol`, `topic`, `feed`, `ts`, `payload` (object), `hostname`, `app_version`, `schema_version`, and `seq`.
- Example: `SINK=ndjson NDJSON_DEST=stdout RUST_LOG=warn cargo run --release | head -n1 | jq .`

Filtering and sampling (env only)
- Include only specific symbols/types in NDJSON via comma-separated patterns (supports `*` suffix wildcard): `NDJSON_INCLUDE='T:ES*,Q:ES*'`
- Sample quotes to reduce volume (emit 1 in N): `NDJSON_SAMPLE_QUOTES=10`

ZMQ PUB sink
- Build with feature: `cargo run --release --features zmq-sink`
- Configure:
  - `SINK=zmq`
  - `ZMQ_ENDPOINT=tcp://127.0.0.1:5556` and optionally `ZMQ_BIND=true`
  - Optional `ZMQ_SND_HWM` to set ZeroMQ high water mark and `ZMQ_TOPIC_PREFIX` to prefix topics.
- Messages are sent as multipart `[topic, payload]` where:
  - `topic` = `"T:SYMBOL"` or `"Q:SYMBOL"` (with optional `ZMQ_TOPIC_PREFIX`)
  - `payload` = NDJSON event (same schema as file/stdout)
 - Monitor metrics: `polygon_zmq_sent_total` (sent) and `polygon_zmq_drops_total` (dropped due to backpressure).

Docker with ZMQ feature
- Build with ZeroMQ enabled: `docker build --build-arg ENABLE_ZMQ_SINK=1 -t polygon-rs:zmq .`

## Effective Config

Enable one‑time config logging:
```bash
RUST_LOG=warn,polygon_config=info cargo run --release
```

## Troubleshooting

- Authentication failed
  - Ensure `POLYGON_API_KEY` is set and valid for the selected `POLYGON_CLUSTER`.
  - Run with `RUST_LOG=info` to see status/auth messages.
- Connection errors / reconnect loops
  - Validate outbound connectivity and DNS. Verify cluster URL mapping.
  - Check metrics: `polygon_reconnects_total`, `polygon_reconnect_failure_total`.
- Read timeouts
  - Increase `READ_TIMEOUT_SECS`, verify subscription has traffic.
  - Tune `HEARTBEAT_INTERVAL_SECS` if needed.
- Readiness never READY
  - Ensure messages are flowing (topic matches cluster). Adjust `READY_MIN_MESSAGES` / `READY_DELAY_MS`.
- Backpressure drops (processing or NDJSON)
  - Increase `CHANNEL_CAP` / `NDJSON_CHANNEL_CAP`, enable rotation (`NDJSON_MAX_BYTES`).
  - Reduce volume with `NDJSON_INCLUDE` / `NDJSON_SAMPLE_QUOTES`.
  - Watch `polygon_channel_drops_total` and `polygon_ndjson_drops_total`.

## Error Handling

- Automatic reconnection on connection loss
- Logging of all critical events
- Graceful shutdown on Ctrl+C

### Health and Metrics
- Health: `curl http://127.0.0.1:9898/health` -> `200 OK` with `OK`
- Readiness: `curl http://127.0.0.1:9898/ready` -> `200 READY` after successful subscribe (503 otherwise)
- Metrics: `curl http://127.0.0.1:9898/metrics` (Prometheus text)
  - Counters include `polygon_reconnects_total`, `polygon_reconnect_success_total`, `polygon_reconnect_failure_total`,
    `polygon_read_timeouts_total`, `polygon_heartbeats_sent_total`, `polygon_messages_received_total`,
    `polygon_trades_total`, `polygon_quotes_total`, `polygon_errors_total`, and `polygon_build_info{...} 1`.

## Contributing

1. Fork the repository
2. Create your feature branch
3. Submit a pull request

## License

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
