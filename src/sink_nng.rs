#[cfg(feature = "nng-sink")]
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
    nng::{Socket, Protocol, options::Options},
};

#[cfg(feature = "nng-sink")]
fn make_socket(
    endpoint: &str,
    bind: bool,
    snd_buf_size: Option<usize>,
) -> Result<Socket, String> {
    let sock = Socket::new(Protocol::Pub0).map_err(|e| e.to_string())?;

    if let Some(buf_size) = snd_buf_size {
        sock.set_opt::<nng::options::SendBufferSize>(buf_size as i32)
            .map_err(|e| e.to_string())?;
    }

    // Set non-blocking mode for sends
    sock.set_opt::<nng::options::SendTimeout>(Some(Duration::from_millis(0)))
        .map_err(|e| e.to_string())?;

    if bind {
        sock.listen(endpoint).map_err(|e| e.to_string())?;
    } else {
        sock.dial(endpoint).map_err(|e| e.to_string())?;
    }

    Ok(sock)
}

#[cfg(feature = "nng-sink")]
pub fn spawn_nng_publisher(
    mut rx: Receiver<NdjsonEvent>,
    endpoint: String,
    bind: bool,
    snd_buf_size: Option<usize>,
    topic_prefix: String,
    warn_interval: Duration,
    metrics: Metrics,
    notify_shutdown: Arc<Notify>,
    init_tx: Option<oneshot::Sender<bool>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_warn = Instant::now() - warn_interval;
        let mut drops_since_warn: u64 = 0;

        let socket = match make_socket(&endpoint, bind, snd_buf_size) {
            Ok(s) => {
                info!(target: "polygon_sink", mode = "nng", endpoint = %endpoint, bind = bind, "nng_ready");
                if let Some(tx) = init_tx {
                    let _ = tx.send(true);
                }
                s
            }
            Err(emsg) => {
                error!(target: "polygon_sink", error = %emsg, endpoint = %endpoint, bind = bind, "nng_socket_error");
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
                    info!(target: "polygon_sink", "nng_shutdown");
                    break;
                }
                maybe = rx.recv() => {
                    match maybe {
                        Some(ev) => {
                            // Build topic as prefix for message
                            topic_buf.clear();
                            let symbol_overhead = if ev.symbol.is_empty() {
                                0
                            } else {
                                1 + ev.symbol.len() // ':' separator + symbol
                            };
                            let need = topic_prefix.len() + ev.r#type.len() + symbol_overhead + 1; // +1 for space separator
                            if topic_buf.capacity() < need {
                                topic_buf.reserve(need - topic_buf.capacity());
                            }
                            topic_buf.extend_from_slice(topic_prefix.as_bytes());
                            topic_buf.extend_from_slice(ev.r#type.as_bytes());
                            if !ev.symbol.is_empty() {
                                topic_buf.push(b':');
                                topic_buf.extend_from_slice(ev.symbol.as_bytes());
                            }
                            topic_buf.push(b' ');

                            // Serialize payload
                            let payload_json = match to_vec(&ev) {
                                Ok(v) => v,
                                Err(e) => {
                                    metrics.inc_error();
                                    warn!(target: "polygon_sink", error = %e, "nng_serialize_error");
                                    continue;
                                }
                            };

                            // Combine topic and payload into single message
                            let mut msg_buf = Vec::with_capacity(topic_buf.len() + payload_json.len() + 1);
                            msg_buf.extend_from_slice(&topic_buf);
                            msg_buf.extend_from_slice(&payload_json);
                            msg_buf.push(b'\n');

                            // Non-blocking send; drop on would-block
                            let msg = nng::Message::from(&msg_buf[..]);
                            match socket.send(msg) {
                                Ok(()) => {
                                    metrics.inc_nng_sent();
                                }
                                Err((_, nng::Error::TryAgain)) => {
                                    // Drop on backpressure (would block)
                                    drops_since_warn = drops_since_warn.saturating_add(1);
                                    metrics.inc_nng_drop();
                                    if last_warn.elapsed() >= warn_interval {
                                        warn!(target: "polygon_sink", dropped = drops_since_warn, interval_secs = warn_interval.as_secs(), "nng_backpressure");
                                        drops_since_warn = 0;
                                        last_warn = Instant::now();
                                    }
                                }
                                Err((_, e)) => {
                                    error!(target: "polygon_sink", error = %e, "nng_send_error");
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
#[cfg(not(feature = "nng-sink"))]
#[allow(unused_variables)]
pub fn spawn_nng_publisher(
    rx: tokio::sync::mpsc::Receiver<crate::ndjson::NdjsonEvent>,
    endpoint: String,
    bind: bool,
    snd_buf_size: Option<usize>,
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
        tracing::error!(target: "polygon_sink", endpoint = %endpoint, bind = bind, "nng feature not enabled; rebuild with --features nng-sink");
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
