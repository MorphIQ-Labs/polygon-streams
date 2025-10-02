#[cfg(feature = "zmq-sink")]
use {
    zmq::{Context, Socket, SocketType, DONTWAIT},
    serde_json::to_vec,
    tokio::sync::mpsc::Receiver,
    tokio::sync::Notify,
    tracing::{error, info, warn},
    std::sync::Arc,
    std::time::{Duration, Instant},
    crate::metrics::Metrics,
    crate::ndjson::NdjsonEvent,
};

#[cfg(feature = "zmq-sink")]
fn make_socket(ctx: &Context, endpoint: &str, bind: bool, snd_hwm: Option<i32>) -> Result<Socket, Box<dyn std::error::Error>> {
    let sock = ctx.socket(SocketType::PUB)?;
    if let Some(hwm) = snd_hwm { sock.set_sndhwm(hwm)?; }
    if bind { sock.bind(endpoint)?; } else { sock.connect(endpoint)?; }
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
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let ctx = Context::new();
        let mut last_warn = Instant::now() - warn_interval;
        let mut drops_since_warn: u64 = 0;

        let socket = match make_socket(&ctx, &endpoint, bind, snd_hwm) {
            Ok(s) => {
                info!(target: "polygon_sink", mode = "zmq", endpoint = %endpoint, bind = bind, "zmq_ready");
                s
            }
            Err(e) => {
                error!(target: "polygon_sink", error = %e, endpoint = %endpoint, bind = bind, "zmq_socket_error");
                // Drop receiver to avoid backpressure and exit this task.
                // Upstream senders will observe a closed channel and account errors.
                drop(rx);
                return;
            }
        };

        // Main pump loop
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
                            let topic = if ev.symbol.is_empty() {
                                format!("{}{}", topic_prefix, ev.r#type)
                            } else {
                                format!("{}{}:{}", topic_prefix, ev.r#type, ev.symbol)
                            };
                            let payload = match to_vec(&ev) {
                                Ok(mut v) => { v.push(b'\n'); v },
                                Err(e) => {
                                    metrics.inc_error();
                                    warn!(target: "polygon_sink", error = %e, "zmq_serialize_error");
                                    continue;
                                }
                            };
                            // Non-blocking send; drop on EAGAIN
                            match socket.send_multipart(&[topic.as_bytes(), &payload], DONTWAIT) {
                                Ok(()) => { metrics.inc_zmq_sent(); }
                                Err(e) => {
                                    // Drop on backpressure (EAGAIN)
                                    if matches!(e, zmq::Error::EAGAIN) {
                                        drops_since_warn = drops_since_warn.saturating_add(1);
                                        metrics.inc_zmq_drop();
                                        if last_warn.elapsed() >= warn_interval {
                                            warn!(target: "polygon_sink", dropped = drops_since_warn, interval_secs = warn_interval.as_secs(), "zmq_backpressure");
                                            drops_since_warn = 0; last_warn = Instant::now();
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
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = (rx, endpoint, bind, snd_hwm, topic_prefix, warn_interval, metrics, notify_shutdown);
        tracing::error!(target: "polygon_sink", "zmq feature not enabled; rebuild with --features zmq-sink");
    })
}
