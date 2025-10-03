// src/main.rs

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use std::time::Duration;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

use crate::cluster::ClusterHandler;
use crate::config::Config;
use crate::metrics::{spawn_http_server, spawn_stats_logger, Metrics};
use crate::model::RequestMessage;
use crate::ndjson::{derive_split_paths, now_millis, spawn_ndjson_writer, NdjsonDest, NdjsonEvent};

mod cluster;
mod config;
mod metrics;
mod model;
mod ndjson;
mod sink_zmq;
mod sink_nng;

// env-only helpers are defined in config.rs

#[derive(Clone)]
struct IncludePattern {
    kind: char,
    prefix: String,
    wildcard: bool,
}

fn parse_include_patterns(s: &str) -> Vec<IncludePattern> {
    let mut v = Vec::new();
    for raw in s.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let mut parts = s.splitn(2, ':');
        let kind_s = parts.next().unwrap_or("");
        let rest = parts.next().unwrap_or("");
        let kind = kind_s.chars().next().unwrap_or('*');
        let wildcard = rest.ends_with('*');
        let prefix = if wildcard {
            rest.trim_end_matches('*').to_string()
        } else {
            rest.to_string()
        };
        v.push(IncludePattern {
            kind,
            prefix,
            wildcard,
        });
    }
    v
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured tracing (default warn; override with RUST_LOG)
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .json()
        .init();

    // Version banner
    info!(target: "polygon_config", app_version = env!("CARGO_PKG_VERSION"), "starting");

    // Load configuration from environment (env-only)
    let mut cfg = Config::from_env()?;

    // Create a bounded channel for message passing (applies backpressure)
    let (tx, rx) = mpsc::channel::<Message>(cfg.channel_capacity);

    // Create a shared notification for graceful shutdown
    let notify_shutdown = Arc::new(Notify::new());

    // Metrics
    let metrics = Metrics::new(
        cfg.cluster.clone(),
        cfg.subscription.clone(),
        cfg.reconnect_buckets.clone(),
        cfg.interarrival_buckets.clone(),
    );

    // Spawn HTTP server for /metrics and /health
    let _http_handle = spawn_http_server(
        metrics.clone(),
        cfg.metrics_addr.clone(),
        notify_shutdown.clone(),
    );
    let _stats_handle =
        spawn_stats_logger(metrics.clone(), notify_shutdown.clone(), cfg.stats_interval);

    // Hostname for enrichment
    let host = Arc::new(
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
    );

    // Build cluster handler
    let handler: Arc<dyn ClusterHandler> = cluster::build_handler(&cfg.cluster).into();

    // Sink selection: stdout | ndjson | zmq
    let sink_kind = cfg.sink.to_ascii_lowercase();

    // Apply conservative defaults for rotation if writing to file and not explicitly set
    if (sink_kind == "ndjson" || sink_kind == "stdout")
        && cfg.ndjson_dest.to_ascii_lowercase() != "stdout"
        && cfg.ndjson_max_bytes.is_none()
        && cfg.ndjson_rotate_secs.is_none()
    {
        cfg.ndjson_max_bytes = Some(100 * 1024 * 1024);
    }

    let (ndjson_tx_opt, ndjson_trades_tx_opt, ndjson_quotes_tx_opt) =
        if sink_kind == "ndjson" || sink_kind == "stdout" {
            // For stdout, force stdout destination
            let dest_override = if sink_kind == "stdout" {
                Some("stdout".to_string())
            } else {
                None
            };
            let ndjson_dest = dest_override.as_deref().unwrap_or(&cfg.ndjson_dest);

            if cfg.ndjson_split_per_type && ndjson_dest.to_ascii_lowercase() != "stdout" {
                let (trades_path, quotes_path) = derive_split_paths(ndjson_dest);
                let (tx_t, rx_t) = mpsc::channel::<NdjsonEvent>(cfg.ndjson_channel_cap);
                let (tx_q, rx_q) = mpsc::channel::<NdjsonEvent>(cfg.ndjson_channel_cap);
                let handle_t = spawn_ndjson_writer(
                    rx_t,
                    NdjsonDest::from_str(&trades_path),
                    metrics.clone(),
                    cfg.ndjson_warn_interval,
                    cfg.ndjson_max_bytes,
                    cfg.ndjson_rotate_secs,
                    cfg.ndjson_gzip,
                    cfg.ndjson_reopen_on_hup,
                    cfg.ndjson_flush_every,
                    cfg.ndjson_flush_interval,
                    notify_shutdown.clone(),
                );
                let handle_q = spawn_ndjson_writer(
                    rx_q,
                    NdjsonDest::from_str(&quotes_path),
                    metrics.clone(),
                    cfg.ndjson_warn_interval,
                    cfg.ndjson_max_bytes,
                    cfg.ndjson_rotate_secs,
                    cfg.ndjson_gzip,
                    cfg.ndjson_reopen_on_hup,
                    cfg.ndjson_flush_every,
                    cfg.ndjson_flush_interval,
                    notify_shutdown.clone(),
                );
                let _ = (handle_t, handle_q);
                (None, Some(tx_t), Some(tx_q))
            } else {
                let (tx_ev, rx_ev) = mpsc::channel::<NdjsonEvent>(cfg.ndjson_channel_cap);
                let handle = spawn_ndjson_writer(
                    rx_ev,
                    NdjsonDest::from_str(ndjson_dest),
                    metrics.clone(),
                    cfg.ndjson_warn_interval,
                    cfg.ndjson_max_bytes,
                    cfg.ndjson_rotate_secs,
                    cfg.ndjson_gzip,
                    cfg.ndjson_reopen_on_hup,
                    cfg.ndjson_flush_every,
                    cfg.ndjson_flush_interval,
                    notify_shutdown.clone(),
                );
                let _ = handle;
                (Some(tx_ev), None, None)
            }
        } else if sink_kind == "zmq" {
            let (tx_ev, rx_ev) = mpsc::channel::<NdjsonEvent>(cfg.zmq_channel_cap);
            let (init_tx, init_rx) = tokio::sync::oneshot::channel();
            let _handle = sink_zmq::spawn_zmq_publisher(
                rx_ev,
                cfg.zmq_endpoint.clone(),
                cfg.zmq_bind,
                cfg.zmq_snd_hwm,
                cfg.zmq_topic_prefix.clone(),
                cfg.zmq_warn_interval,
                metrics.clone(),
                notify_shutdown.clone(),
                Some(init_tx),
            );
            match timeout(Duration::from_secs(2), init_rx).await {
                Ok(Ok(true)) => { /* ok */ }
                Ok(Ok(false)) => {
                    warn!(target: "polygon_sink", "zmq_init_failed");
                }
                Ok(Err(_)) => {
                    warn!(target: "polygon_sink", "zmq_init_channel_closed");
                }
                Err(_) => {
                    warn!(target: "polygon_sink", "zmq_init_timeout");
                }
            }
            (Some(tx_ev), None, None)
        } else if sink_kind == "nng" {
            let (tx_ev, rx_ev) = mpsc::channel::<NdjsonEvent>(cfg.nng_channel_cap);
            let (init_tx, init_rx) = tokio::sync::oneshot::channel();
            let _handle = sink_nng::spawn_nng_publisher(
                rx_ev,
                cfg.nng_endpoint.clone(),
                cfg.nng_bind,
                cfg.nng_snd_buf_size,
                cfg.nng_topic_prefix.clone(),
                cfg.nng_warn_interval,
                metrics.clone(),
                notify_shutdown.clone(),
                Some(init_tx),
            );
            match timeout(Duration::from_secs(2), init_rx).await {
                Ok(Ok(true)) => { /* ok */ }
                Ok(Ok(false)) => {
                    warn!(target: "polygon_sink", "nng_init_failed");
                }
                Ok(Err(_)) => {
                    warn!(target: "polygon_sink", "nng_init_channel_closed");
                }
                Err(_) => {
                    warn!(target: "polygon_sink", "nng_init_timeout");
                }
            }
            (Some(tx_ev), None, None)
        } else {
            (None, None, None)
        };

    // Spawn a task to process incoming messages
    // Compile include patterns for NDJSON if provided
    let include_patterns = cfg
        .ndjson_include
        .as_ref()
        .map(|s| parse_include_patterns(&s.join(",")));
    let processor_handle = spawn_message_processor(
        rx,
        notify_shutdown.clone(),
        metrics.clone(),
        ndjson_tx_opt.as_ref().map(|tx| tx.clone()),
        ndjson_trades_tx_opt.as_ref().map(|tx| tx.clone()),
        ndjson_quotes_tx_opt.as_ref().map(|tx| tx.clone()),
        if sink_kind == "zmq" {
            cfg.zmq_warn_interval
        } else if sink_kind == "nng" {
            cfg.nng_warn_interval
        } else {
            cfg.ndjson_warn_interval
        },
        if sink_kind == "zmq" {
            SinkType::Zmq
        } else if sink_kind == "nng" {
            SinkType::Nng
        } else {
            SinkType::Ndjson
        },
        include_patterns,
        cfg.ndjson_sample_quotes.max(1),
        host.clone(),
        handler.clone(),
    );

    // Handle graceful shutdown on Ctrl+C
    let shutdown_handle = handle_shutdown(notify_shutdown.clone());

    // Reconnect loop with exponential backoff
    let mut backoff = cfg.backoff_initial;
    let max_backoff = cfg.backoff_max;

    loop {
        metrics.inc_reconnect();
        metrics.set_ready(false);
        let attempt_start = std::time::Instant::now();
        let ws_url = handler.ws_url().to_string();
        info!("Connecting to {}", ws_url);
        let socket = match connect_async(&ws_url).await {
            Ok((socket, response)) => {
                info!("Connected with status: {}", response.status());
                socket
            }
            Err(err) => {
                warn!("Connect failed: {}", err);
                metrics.inc_error();
                metrics.inc_reconnect_failure();
                if wait_or_shutdown(backoff, notify_shutdown.clone()).await {
                    break;
                }
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        // Authenticate with the API key
        let socket = match authenticate(socket, &cfg.api_key).await {
            Ok(s) => s,
            Err(err) => {
                warn!("Authentication failed: {}", err);
                metrics.inc_error();
                metrics.inc_reconnect_failure();
                if wait_or_shutdown(backoff, notify_shutdown.clone()).await {
                    break;
                }
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        // Subscribe to the specified topic
        let effective_sub = if cfg.subscription.is_empty() {
            handler.default_subscription().to_string()
        } else {
            cfg.subscription.clone()
        };
        let socket = match subscribe(socket, &effective_sub).await {
            Ok(s) => s,
            Err(err) => {
                warn!("Subscribe failed: {}", err);
                metrics.inc_error();
                metrics.inc_reconnect_failure();
                if wait_or_shutdown(backoff, notify_shutdown.clone()).await {
                    break;
                }
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        // Reset backoff after successful connection + auth + subscribe
        backoff = Duration::from_secs(1);
        metrics.inc_reconnect_success();
        metrics.observe_reconnect_success_secs(attempt_start.elapsed().as_secs_f64());

        // Spawn a task to read messages from the socket and send them to the processor
        let reader_handle = spawn_socket_reader(
            socket,
            tx.clone(),
            notify_shutdown.clone(),
            cfg.read_timeout,
            cfg.heartbeat_interval,
            metrics.clone(),
            cfg.ready_min_messages,
            cfg.ready_delay,
            cfg.bp_warn_interval,
        );

        // Wait for either shutdown or reader exit, then attempt reconnect
        tokio::select! {
            _ = notify_shutdown.notified() => {
                info!("Shutdown signal received.");
                break;
            }
            res = reader_handle => {
                match res {
                    Ok(_) => info!("Socket reader exited; will attempt reconnect."),
                    Err(err) => error!("Socket reader task panicked: {}", err),
                }
            }
        }

        if wait_or_shutdown(backoff, notify_shutdown.clone()).await {
            break;
        }
        backoff = (backoff * 2).min(max_backoff);
    }

    // Cleanup: close sender to end processor, then await tasks
    drop(tx);
    let _ = processor_handle.await;
    let _ = shutdown_handle.await;

    info!("Shutting down gracefully.");
    Ok(())
}

// No CLI flags struct; env-only configuration.

async fn authenticate<S>(
    mut socket: WebSocketStream<S>,
    api_key: &str,
) -> Result<WebSocketStream<S>, Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    info!("Authenticating...");

    // Send authentication message
    let auth_message = RequestMessage {
        action: "auth",
        params: api_key,
    };
    let auth_message_json = serde_json::to_string(&auth_message)?;
    debug!("Authentication message: {}", &auth_message_json);
    socket.send(Message::Text(auth_message_json.into())).await?;

    // Read authentication response
    if let Some(message) = socket.next().await {
        match message {
            Ok(msg) => {
                debug!("Authentication response: {}", msg);
                if let Message::Text(text) = msg {
                    let vals: Vec<serde_json::Value> = serde_json::from_str(&text)?;
                    for v in vals {
                        if v.get("ev").and_then(|x| x.as_str()) == Some("status") {
                            let status = v.get("status").and_then(|x| x.as_str()).unwrap_or("");
                            info!("Status message: {}", status);
                            if status == "auth_failed" {
                                let msg = v.get("message").and_then(|x| x.as_str()).unwrap_or("");
                                error!("Authentication failed: {}", msg);
                                return Err("Authentication failed.".into());
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!("Error during authentication: {}", err);
                return Err(err.into());
            }
        }
    } else {
        error!("No authentication response received.");
        return Err("No authentication response received.".into());
    }

    Ok(socket)
}

async fn subscribe<S>(
    mut socket: WebSocketStream<S>,
    ticker: &str,
) -> Result<WebSocketStream<S>, Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    info!("Subscribing to ticker: {}", ticker);

    // Send subscribe message
    let subscribe_message = RequestMessage {
        action: "subscribe",
        params: ticker,
    };
    let subscribe_message_json = serde_json::to_string(&subscribe_message)?;
    debug!("Subscribe message: {}", &subscribe_message_json);
    socket
        .send(Message::Text(subscribe_message_json.into()))
        .await?;

    // Read subscription response (generic)
    if let Some(message) = socket.next().await {
        match message {
            Ok(msg) => {
                debug!("Subscription response: {}", msg);
                if let Message::Text(text) = msg {
                    let vals: Vec<serde_json::Value> = serde_json::from_str(&text)?;
                    for v in vals {
                        if v.get("ev").and_then(|x| x.as_str()) == Some("status") {
                            let status = v.get("status").and_then(|x| x.as_str()).unwrap_or("");
                            info!("Status message: {}", status);
                            if status == "error" || status == "auth_failed" {
                                let msg = v.get("message").and_then(|x| x.as_str()).unwrap_or("");
                                error!("Subscription failed: {}", msg);
                                return Err("Subscription failed.".into());
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!("Error during subscription: {}", err);
                return Err(err.into());
            }
        }
    } else {
        error!("No subscription response received.");
        return Err("No subscription response received.".into());
    }

    Ok(socket)
}

enum SinkType {
    Ndjson,
    Zmq,
    Nng,
}

struct NdjsonBp {
    last_warn: Instant,
    dropped_since_warn: u64,
    warn_interval: Duration,
    include: Option<Vec<IncludePattern>>,
    sample_quotes_n: u64,
    quotes_counter: u64,
    seq_counter: u64,
    sink_type: SinkType,
}

impl NdjsonBp {
    fn new(
        warn_interval: Duration,
        include: Option<Vec<IncludePattern>>,
        sample_quotes_n: u64,
        sink_type: SinkType,
    ) -> Self {
        NdjsonBp {
            last_warn: Instant::now() - warn_interval,
            dropped_since_warn: 0,
            warn_interval,
            include,
            sample_quotes_n,
            quotes_counter: 0,
            seq_counter: 0,
            sink_type,
        }
    }
}

fn record_sink_backpressure(metrics: &Metrics, bp: &mut NdjsonBp) {
    match bp.sink_type {
        SinkType::Zmq => metrics.inc_zmq_drop(),
        SinkType::Nng => metrics.inc_nng_drop(),
        SinkType::Ndjson => metrics.inc_ndjson_drop(),
    }
    bp.dropped_since_warn = bp.dropped_since_warn.saturating_add(1);
    if bp.last_warn.elapsed() >= bp.warn_interval {
        match bp.sink_type {
            SinkType::Zmq => {
                warn!(target: "polygon_sink", dropped = bp.dropped_since_warn, interval_secs = bp.warn_interval.as_secs(), "zmq_backpressure");
            }
            SinkType::Nng => {
                warn!(target: "polygon_sink", dropped = bp.dropped_since_warn, interval_secs = bp.warn_interval.as_secs(), "nng_backpressure");
            }
            SinkType::Ndjson => {
                warn!(target: "polygon_ndjson", dropped = bp.dropped_since_warn, interval_secs = bp.warn_interval.as_secs(), "ndjson_backpressure");
            }
        }
        bp.dropped_since_warn = 0;
        bp.last_warn = Instant::now();
    }
}

fn make_ndjson_event(
    typ: &str,
    symbol: &str,
    ts: i64,
    payload: serde_json::Value,
    metrics: &Metrics,
    host: &str,
    seq: u64,
) -> NdjsonEvent {
    NdjsonEvent {
        ingest_ts: now_millis(),
        r#type: ndjson::intern_event_type(typ),
        symbol: symbol.to_string(),
        topic: metrics.topic().to_string(),
        feed: metrics.feed().to_string(),
        ts,
        payload,
        hostname: host.to_string(),
        app_version: env!("CARGO_PKG_VERSION"),
        schema_version: 1,
        seq,
    }
}

fn spawn_message_processor(
    mut rx: Receiver<Message>,
    notify_shutdown: Arc<Notify>,
    metrics: Metrics,
    ndjson_tx: Option<Sender<NdjsonEvent>>,
    ndjson_trades_tx: Option<Sender<NdjsonEvent>>,
    ndjson_quotes_tx: Option<Sender<NdjsonEvent>>,
    ndjson_warn_interval: Duration,
    sink_type: SinkType,
    include_patterns: Option<Vec<IncludePattern>>,
    sample_quotes_n: u64,
    host: Arc<String>,
    handler: Arc<dyn ClusterHandler>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ndjson_bp = NdjsonBp::new(
            ndjson_warn_interval,
            include_patterns,
            sample_quotes_n,
            sink_type,
        );
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(message) => {
                            if let Err(err) = process_message(message, &metrics, ndjson_tx.as_ref(), ndjson_trades_tx.as_ref(), ndjson_quotes_tx.as_ref(), &mut ndjson_bp, &host, handler.as_ref()) {
                                error!("Error processing message: {}", err);
                                metrics.inc_error();
                            }
                        }
                        None => {
                            info!("Message channel closed; processor exiting.");
                            break;
                        }
                    }
                }
                _ = notify_shutdown.notified() => {
                    info!("Message processor received shutdown signal.");
                    break;
                }
            }
        }
    })
}

fn spawn_socket_reader<S>(
    socket: WebSocketStream<S>,
    tx: Sender<Message>,
    notify_shutdown: Arc<Notify>,
    read_timeout: Duration,
    heartbeat_interval: Option<Duration>,
    metrics: Metrics,
    ready_min_messages: u64,
    ready_delay: Duration,
    bp_warn_interval: Duration,
) -> JoinHandle<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut socket_writer, mut socket_reader) = socket.split();

    tokio::spawn(async move {
        let heartbeat_enabled = heartbeat_interval.is_some();
        let mut hb = interval(heartbeat_interval.unwrap_or(Duration::from_secs(3600)));
        hb.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let start = std::time::Instant::now();
        let mut msgs_since = 0u64;
        let mut last_rx: Option<std::time::Instant> = None;
        let mut last_warn = Instant::now() - bp_warn_interval;
        let mut dropped_since_warn: u64 = 0;
        loop {
            tokio::select! {
                res = timeout(read_timeout, socket_reader.next()) => {
                    match res {
                        Ok(Some(Ok(msg))) => {
                            metrics.inc_received();
                            msgs_since = msgs_since.saturating_add(1);
                            let now = std::time::Instant::now();
                            if let Some(prev) = last_rx { metrics.observe_interarrival_secs(now.duration_since(prev).as_secs_f64()); }
                            last_rx = Some(now);
                            if !metrics.is_ready() && msgs_since >= ready_min_messages && (ready_delay.is_zero() || start.elapsed() >= ready_delay) {
                                metrics.set_ready(true);
                            }
                            match tx.try_send(msg) {
                                Ok(()) => {}
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_msg)) => {
                                    metrics.inc_drop();
                                    dropped_since_warn = dropped_since_warn.saturating_add(1);
                                    if last_warn.elapsed() >= bp_warn_interval {
                                        warn!(dropped = dropped_since_warn, interval_secs = bp_warn_interval.as_secs(), "backpressure");
                                        dropped_since_warn = 0;
                                        last_warn = Instant::now();
                                    }
                                    // drop the message and continue
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_msg)) => {
                                    error!("Processor channel closed; reader exiting");
                                    metrics.inc_error();
                                    break;
                                }
                            }
                        }
                        Ok(Some(Err(err))) => {
                            warn!("Error reading message from socket: {}", err);
                            metrics.inc_error();
                            metrics.set_ready(false);
                            break;
                        }
                        Ok(None) => {
                            info!("WebSocket stream closed by server.");
                            metrics.set_ready(false);
                            break;
                        }
                        Err(_) => {
                            warn!("Read timeout after {:?}; reconnecting", read_timeout);
                            metrics.inc_timeout();
                            metrics.set_ready(false);
                            break;
                        }
                    }
                }
                _ = notify_shutdown.notified() => {
                    info!("Socket reader received shutdown signal.");
                    metrics.set_ready(false);
                    break;
                }
                _ = hb.tick(), if heartbeat_enabled => {
                    if let Err(err) = socket_writer.send(Message::Ping(Vec::new().into())).await {
                        warn!("Heartbeat ping failed: {}", err);
                        metrics.inc_error();
                        break;
                    } else {
                        metrics.inc_heartbeat();
                        debug!("Sent heartbeat ping");
                    }
                }
            }
        }

        // Optionally, close the writer half gracefully
        if let Err(err) = socket_writer.close().await {
            error!("Error closing socket writer: {}", err);
        }
    })
}

fn handle_shutdown(notify_shutdown: Arc<Notify>) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Wait for Ctrl+C signal
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C: {}", err);
        }
        // Notify other tasks to shut down
        notify_shutdown.notify_waiters();
    })
}

fn process_message(
    message: Message,
    metrics: &Metrics,
    ndjson_tx: Option<&Sender<NdjsonEvent>>,
    ndjson_trades_tx: Option<&Sender<NdjsonEvent>>,
    ndjson_quotes_tx: Option<&Sender<NdjsonEvent>>,
    ndjson_bp: &mut NdjsonBp,
    host: &Arc<String>,
    handler: &dyn ClusterHandler,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Message::Text(text) = message {
        let events = handler.normalize_messages(&text)?;
        let mut buf: Vec<u8> = Vec::with_capacity(256);
        for ev in events {
            match ev.ev.as_str() {
                "status" => {
                    metrics.inc_status();
                    continue;
                }
                "T" => {
                    metrics.inc_trade();
                }
                "Q" => {
                    metrics.inc_quote();
                }
                _ => {}
            }

            let symbol = ev.symbol.as_deref().unwrap_or("");

            // Detailed event logging (guarded to avoid expensive operations when disabled)
            if tracing::event_enabled!(target: "polygon_events", tracing::Level::DEBUG) {
                buf.clear();
                buf.extend_from_slice(ev.ev.as_bytes());
                buf.push(b':');
                if let Some(sym) = &ev.symbol {
                    buf.extend_from_slice(sym.as_bytes());
                }
                buf.push(b' ');
                if let Ok(payload_json) = serde_json::to_string(&ev.payload) {
                    buf.extend_from_slice(payload_json.as_bytes());
                    if let Ok(line) = std::str::from_utf8(&buf) {
                        debug!(target: "polygon_events", r#type = %ev.ev, symbol = %symbol, ts = ev.ts.unwrap_or(0), payload = %payload_json, line = %line);
                    }
                }
            }

            // NDJSON emission
            let choose_tx = match ev.ev.as_str() {
                "T" => ndjson_trades_tx.or(ndjson_tx),
                "Q" => ndjson_quotes_tx.or(ndjson_tx),
                _ => ndjson_tx,
            };
            if let Some(tx) = choose_tx {
                // Sampling for quotes
                if ev.ev.as_str() == "Q" {
                    ndjson_bp.quotes_counter = ndjson_bp.quotes_counter.saturating_add(1);
                    if ndjson_bp.sample_quotes_n > 1
                        && (ndjson_bp.quotes_counter % ndjson_bp.sample_quotes_n != 0)
                    {
                        continue;
                    }
                }
                // Include filters
                if let Some(ref pats) = ndjson_bp.include {
                    let kind = ev.ev.chars().next().unwrap_or('*');
                    let mut matched = false;
                    for p in pats.iter() {
                        if p.kind == kind || p.kind == '*' {
                            if p.wildcard {
                                if symbol.starts_with(&p.prefix) {
                                    matched = true;
                                    break;
                                }
                            } else if symbol == p.prefix {
                                matched = true;
                                break;
                            }
                        }
                    }
                    if !matched {
                        continue;
                    }
                }
                ndjson_bp.seq_counter = ndjson_bp.seq_counter.saturating_add(1);
                let ev_out = make_ndjson_event(
                    &ev.ev,
                    symbol,
                    ev.ts.unwrap_or(0),
                    ev.payload,
                    metrics,
                    host,
                    ndjson_bp.seq_counter,
                );
                match tx.try_send(ev_out) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        record_sink_backpressure(&metrics, ndjson_bp);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        metrics.inc_error();
                    }
                }
            }
        }
    }
    Ok(())
}

async fn wait_or_shutdown(duration: Duration, notify_shutdown: Arc<Notify>) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        _ = notify_shutdown.notified() => true,
    }
}
