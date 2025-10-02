#[cfg(feature = "zmq-sink")]
use {
    crate::metrics::Metrics,
    crate::ndjson::NdjsonEvent,
    serde_json::to_vec,
    smallvec::SmallVec,
    std::sync::Arc,
    std::time::{Duration, Instant},
    tokio::sync::mpsc::Receiver,
    tokio::sync::{oneshot, Notify},
    tracing::{error, info, warn},
    zmq::{Context, Socket, SocketType, DONTWAIT},
};

#[cfg(feature = "zmq-sink")]
fn make_socket(
    ctx: &Context,
    endpoint: &str,
    bind: bool,
    snd_hwm: Option<i32>,
) -> Result<Socket, String> {
    let sock = ctx.socket(SocketType::PUB).map_err(|e| e.to_string())?;

    if let Some(hwm) = snd_hwm {
        sock.set_sndhwm(hwm).map_err(|e| e.to_string())?;
    }

    if bind {
        sock.bind(endpoint).map_err(|e| e.to_string())?;
    } else {
        sock.connect(endpoint).map_err(|e| e.to_string())?;
    }

    Ok(sock)
}

#[cfg(feature = "zmq-sink")]
pub fn spawn_zmq_publisher(
    mut rx: Receiver<NdjsonEvent>,
    endpoint: String,
    bind: bool,
    snd_hwm: Option<i32>,
    topic_prefix: String,
    warn_interval: Duration,
    metrics: Metrics,
    notify_shutdown: Arc<Notify>,
    init_tx: Option<oneshot::Sender<bool>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let ctx = Context::new();
        let mut last_warn = Instant::now() - warn_interval;
        let mut drops_since_warn: u64 = 0;

        let socket = match make_socket(&ctx, &endpoint, bind, snd_hwm) {
            Ok(s) => {
                info!(target: "polygon_sink", mode = "zmq", endpoint = %endpoint, bind = bind, "zmq_ready");
                if let Some(tx) = init_tx {
                    let _ = tx.send(true);
                }
                s
            }
            Err(emsg) => {
                error!(target: "polygon_sink", error = %emsg, endpoint = %endpoint, bind = bind, "zmq_socket_error");
                if let Some(tx) = init_tx {
                    let _ = tx.send(false);
                }
                // Drain until shutdown so we don't stall tasks
                loop {
                    tokio::select! {
                        _ = notify_shutdown.notified() => break,
                        _ = rx.recv() => {}
                    }
                }
                return;
            }
        };

        // Main pump loop
        // Reuse topic buffer (smallvec) to minimize allocations per send
        let mut topic_buf: SmallVec<[u8; 64]> = SmallVec::new();
        loop {
            tokio::select! {
                _ = notify_shutdown.notified() => {
                    info!(target: "polygon_sink", "zmq_shutdown");
                    break;
                }
                maybe = rx.recv() => {
                    match maybe {
                        Some(ev) => {
                            // Multipart: [topic, payload]
                            // Build topic directly as bytes to avoid intermediate String
                            topic_buf.clear();
                            let need = topic_prefix.len() + ev.r#type.len() + if ev.symbol.is_empty() { 0 } else { 1 + ev.symbol.len() };
                            if topic_buf.capacity() < need {
                                topic_buf.reserve(need - topic_buf.capacity());
                            }
                            topic_buf.extend_from_slice(topic_prefix.as_bytes());
                            topic_buf.extend_from_slice(ev.r#type.as_bytes());
                            if !ev.symbol.is_empty() {
                                topic_buf.push(b':');
                                topic_buf.extend_from_slice(ev.symbol.as_bytes());
                            }
                            let payload = match to_vec(&ev) {
                                Ok(mut v) => { v.push(b'\n'); v },
                                Err(e) => {
                                    metrics.inc_error();
                                    warn!(target: "polygon_sink", error = %e, "zmq_serialize_error");
                                    continue;
                                }
                            };
                            // Non-blocking send; drop on EAGAIN
                            match socket.send_multipart(&[&topic_buf[..], &payload], DONTWAIT) {
                                Ok(()) => { metrics.inc_zmq_sent(); }
                                Err(e) => {
                                    // Drop on backpressure (EAGAIN)
                                    if matches!(e, zmq::Error::EAGAIN) {
                                        drops_since_warn = drops_since_warn.saturating_add(1);
                                        metrics.inc_zmq_drop();
                                        if last_warn.elapsed() >= warn_interval {
                                            warn!(target: "polygon_sink", dropped = drops_since_warn, interval_secs = warn_interval.as_secs(), "zmq_backpressure");
                                            drops_since_warn = 0;
                                            last_warn = Instant::now();
                                        }
                                    } else {
                                        error!(target: "polygon_sink", error = %e, "zmq_send_error");
                                    }
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    })
}

// Stubs when the feature is disabled to keep call sites simple.
#[cfg(not(feature = "zmq-sink"))]
#[allow(unused_variables)]
pub fn spawn_zmq_publisher(
    rx: tokio::sync::mpsc::Receiver<crate::ndjson::NdjsonEvent>,
    endpoint: String,
    bind: bool,
    snd_hwm: Option<i32>,
    topic_prefix: String,
    warn_interval: std::time::Duration,
    metrics: crate::metrics::Metrics,
    notify_shutdown: std::sync::Arc<tokio::sync::Notify>,
    init_tx: Option<tokio::sync::oneshot::Sender<bool>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Report init failure to caller explicitly
        if let Some(tx) = init_tx {
            let _ = tx.send(false);
        }
        // Log context (endpoint/bind) for clarity
        tracing::error!(target: "polygon_sink", endpoint = %endpoint, bind = bind, "zmq feature not enabled; rebuild with --features zmq-sink");
        // Gracefully drain until shutdown to avoid backpressure upstream
        let mut rx = rx;
        loop {
            tokio::select! {
                _ = notify_shutdown.notified() => break,
                maybe = rx.recv() => { if maybe.is_none() { break; } }
            }
        }
    })
}
