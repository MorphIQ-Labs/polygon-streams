use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct Metrics {
    reconnects_total: Arc<AtomicU64>,
    reconnect_success_total: Arc<AtomicU64>,
    reconnect_failure_total: Arc<AtomicU64>,
    read_timeouts_total: Arc<AtomicU64>,
    heartbeats_sent_total: Arc<AtomicU64>,
    messages_received_total: Arc<AtomicU64>,
    trades_total: Arc<AtomicU64>,
    quotes_total: Arc<AtomicU64>,
    status_total: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,
    ndjson_drops_total: Arc<AtomicU64>,
    ndjson_written_total: Arc<AtomicU64>,
    channel_drops_total: Arc<AtomicU64>,
    zmq_drops_total: Arc<AtomicU64>,
    zmq_sent_total: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    feed: String,
    topic: String,
    reconnect_hist_success: Arc<Histogram>,
    interarrival_hist: Arc<Histogram>,
}

impl Metrics {
    pub fn new(
        feed: String,
        topic: String,
        reconnect_buckets: Vec<f64>,
        interarrival_buckets: Vec<f64>,
    ) -> Self {
        Metrics {
            reconnects_total: Arc::new(AtomicU64::new(0)),
            reconnect_success_total: Arc::new(AtomicU64::new(0)),
            reconnect_failure_total: Arc::new(AtomicU64::new(0)),
            read_timeouts_total: Arc::new(AtomicU64::new(0)),
            heartbeats_sent_total: Arc::new(AtomicU64::new(0)),
            messages_received_total: Arc::new(AtomicU64::new(0)),
            trades_total: Arc::new(AtomicU64::new(0)),
            quotes_total: Arc::new(AtomicU64::new(0)),
            status_total: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
            ndjson_drops_total: Arc::new(AtomicU64::new(0)),
            ndjson_written_total: Arc::new(AtomicU64::new(0)),
            channel_drops_total: Arc::new(AtomicU64::new(0)),
            zmq_drops_total: Arc::new(AtomicU64::new(0)),
            zmq_sent_total: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(false)),
            feed,
            topic,
            reconnect_hist_success: Arc::new(Histogram::new(reconnect_buckets)),
            interarrival_hist: Arc::new(Histogram::new(interarrival_buckets)),
        }
    }

    pub fn inc_reconnect(&self) {
        self.reconnects_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_reconnect_success(&self) {
        self.reconnect_success_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_reconnect_failure(&self) {
        self.reconnect_failure_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_timeout(&self) {
        self.read_timeouts_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_heartbeat(&self) {
        self.heartbeats_sent_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_received(&self) {
        self.messages_received_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_trade(&self) {
        self.trades_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_quote(&self) {
        self.quotes_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_status(&self) {
        self.status_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_drop(&self) {
        self.channel_drops_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_ndjson_drop(&self) {
        self.ndjson_drops_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_ndjson_written(&self) {
        self.ndjson_written_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_zmq_drop(&self) {
        self.zmq_drops_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_zmq_sent(&self) {
        self.zmq_sent_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_ready(&self, v: bool) {
        self.ready.store(v, Ordering::Relaxed);
    }
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    pub fn render_prometheus(&self) -> String {
        let mut s = Vec::with_capacity(512);
        // Build info
        let name = env!("CARGO_PKG_NAME");
        let version = env!("CARGO_PKG_VERSION");
        let _ = writeln!(&mut s, "# HELP polygon_build_info Build information");
        let _ = writeln!(&mut s, "# TYPE polygon_build_info gauge");
        let _ = writeln!(
            &mut s,
            "polygon_build_info{{name=\"{}\",version=\"{}\"}} 1",
            name, version
        );

        let labels = format!("feed=\"{}\",topic=\"{}\"", self.feed, self.topic);

        let _ = writeln!(&mut s, "# HELP polygon_reconnects_total Reconnect attempts");
        let _ = writeln!(&mut s, "# TYPE polygon_reconnects_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_reconnects_total{{{}}} {}",
            labels,
            self.reconnects_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_reconnect_success_total Successful reconnect cycles"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_reconnect_success_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_reconnect_success_total{{{}}} {}",
            labels,
            self.reconnect_success_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_reconnect_failure_total Failed reconnect attempts before retry"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_reconnect_failure_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_reconnect_failure_total{{{}}} {}",
            labels,
            self.reconnect_failure_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_read_timeouts_total Read timeouts on the WebSocket"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_read_timeouts_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_read_timeouts_total{{{}}} {}",
            labels,
            self.read_timeouts_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_heartbeats_sent_total Heartbeat pings sent"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_heartbeats_sent_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_heartbeats_sent_total{{{}}} {}",
            labels,
            self.heartbeats_sent_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_messages_received_total WebSocket messages received"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_messages_received_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_messages_received_total{{{}}} {}",
            labels,
            self.messages_received_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_trades_total Futures trade messages processed"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_trades_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_trades_total{{{}}} {}",
            labels,
            self.trades_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_quotes_total Futures quote messages processed"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_quotes_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_quotes_total{{{}}} {}",
            labels,
            self.quotes_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_status_total Status messages processed"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_status_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_status_total{{{}}} {}",
            labels,
            self.status_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(&mut s, "# HELP polygon_errors_total Errors encountered");
        let _ = writeln!(&mut s, "# TYPE polygon_errors_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_errors_total{{{}}} {}",
            labels,
            self.errors_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_channel_drops_total Messages dropped due to backpressure"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_channel_drops_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_channel_drops_total{{{}}} {}",
            labels,
            self.channel_drops_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_ndjson_drops_total NDJSON messages dropped due to backpressure"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_ndjson_drops_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_ndjson_drops_total{{{}}} {}",
            labels,
            self.ndjson_drops_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_ndjson_written_total NDJSON messages written"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_ndjson_written_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_ndjson_written_total{{{}}} {}",
            labels,
            self.ndjson_written_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_zmq_drops_total ZMQ send drops due to backpressure"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_zmq_drops_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_zmq_drops_total{{{}}} {}",
            labels,
            self.zmq_drops_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            &mut s,
            "# HELP polygon_zmq_sent_total ZMQ messages sent successfully"
        );
        let _ = writeln!(&mut s, "# TYPE polygon_zmq_sent_total counter");
        let _ = writeln!(
            &mut s,
            "polygon_zmq_sent_total{{{}}} {}",
            labels,
            self.zmq_sent_total.load(Ordering::Relaxed)
        );

        // Histograms
        self.reconnect_hist_success
            .render("polygon_reconnect_duration_seconds", &labels, &mut s);
        self.interarrival_hist
            .render("polygon_message_interarrival_seconds", &labels, &mut s);

        String::from_utf8(s).unwrap_or_default()
    }
}

pub fn spawn_http_server(
    metrics: Metrics,
    addr: String,
    notify_shutdown: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match TcpListener::bind(&addr).await {
            Ok(listener) => {
                info!("metrics_http_listen" = %addr, "Listening for /metrics and /health");
                loop {
                    tokio::select! {
                        _ = notify_shutdown.notified() => {
                            info!("metrics_http_shutdown" = true, "Shutting down metrics server");
                            break;
                        }
                        accept_res = listener.accept() => {
                            match accept_res {
                                Ok((socket, _peer)) => {
                                    let m = metrics.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = handle_conn(socket, m).await {
                                            debug!(error = %e, "metrics_http_conn_error");
                                        }
                                    });
                                }
                                Err(e) => {
                                    warn!(error = %e, "metrics_http_accept_error");
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to bind metrics HTTP server");
            }
        }
    })
}

async fn handle_conn(
    mut socket: TcpStream,
    metrics: Metrics,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 1024];
    let n = socket.read(&mut buf).await?;
    let req = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let (status, content_type, body) =
        if req.starts_with("GET /metrics ") || req.starts_with("GET /metrics\r") {
            (
                "200 OK",
                "text/plain; version=0.0.4",
                metrics.render_prometheus(),
            )
        } else if req.starts_with("GET /health ")
            || req.starts_with("GET /health\r")
            || req.starts_with("GET / ")
        {
            ("200 OK", "text/plain", "OK".to_string())
        } else if req.starts_with("GET /ready ") || req.starts_with("GET /ready\r") {
            if metrics.is_ready() {
                ("200 OK", "text/plain", "READY".to_string())
            } else {
                (
                    "503 Service Unavailable",
                    "text/plain",
                    "NOT READY".to_string(),
                )
            }
        } else {
            ("404 Not Found", "text/plain", "Not Found".to_string())
        };
    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.shutdown().await?;
    Ok(())
}

pub fn spawn_stats_logger(
    metrics: Metrics,
    notify_shutdown: Arc<Notify>,
    period: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last = Snapshot::from(&metrics);
        let mut tick = interval(period);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = notify_shutdown.notified() => break,
                _ = tick.tick() => {
                    let now = Snapshot::from(&metrics);
                    let secs = period.as_secs_f64().max(0.001);
                    info!(target: "polygon_stats",
                        trades = now.trades - last.trades,
                        quotes = now.quotes - last.quotes,
                        status = now.status - last.status,
                        drops = now.drops - last.drops,
                        received = now.received - last.received,
                        timeouts = now.timeouts - last.timeouts,
                        errors = now.errors - last.errors,
                        trades_rate = (now.trades - last.trades) as f64 / secs,
                        quotes_rate = (now.quotes - last.quotes) as f64 / secs,
                        "stats"
                    );
                    last = now;
                }
            }
        }
    })
}

#[derive(Clone, Copy, Default)]
struct Snapshot {
    trades: u64,
    quotes: u64,
    status: u64,
    drops: u64,
    received: u64,
    timeouts: u64,
    errors: u64,
}

impl From<&Metrics> for Snapshot {
    fn from(m: &Metrics) -> Self {
        use std::sync::atomic::Ordering::Relaxed;
        Snapshot {
            trades: m.trades_total.load(Relaxed),
            quotes: m.quotes_total.load(Relaxed),
            status: m.status_total.load(Relaxed),
            drops: m.channel_drops_total.load(Relaxed),
            received: m.messages_received_total.load(Relaxed),
            timeouts: m.read_timeouts_total.load(Relaxed),
            errors: m.errors_total.load(Relaxed),
        }
    }
}

struct Histogram {
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn new(mut buckets: Vec<f64>) -> Self {
        buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let counts = (0..buckets.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets,
            counts,
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn observe(&self, value_secs: f64) {
        let us = if value_secs.is_sign_positive() {
            (value_secs * 1_000_000.0) as u64
        } else {
            0
        };
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        for (i, b) in self.buckets.iter().enumerate() {
            if value_secs <= *b {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    fn render(&self, name: &str, labels: &str, out: &mut Vec<u8>) {
        let _ = writeln!(out, "# HELP {} Histogram of {}", name, name);
        let _ = writeln!(out, "# TYPE {} histogram", name);
        let mut cumulative = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            let c = self.counts[i].load(Ordering::Relaxed);
            cumulative += c;
            let _ = writeln!(
                out,
                "{}_bucket{{{},le=\"{}\"}} {}",
                name, labels, b, cumulative
            );
        }
        let total = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{}_bucket{{{},le=\"+Inf\"}} {}", name, labels, total);
        let sum_secs = (self.sum_us.load(Ordering::Relaxed) as f64) / 1_000_000.0;
        let _ = writeln!(out, "{}_sum{{{}}} {}", name, labels, sum_secs);
        let _ = writeln!(out, "{}_count{{{}}} {}", name, labels, total);
    }
}

impl Metrics {
    pub fn observe_reconnect_success_secs(&self, secs: f64) {
        self.reconnect_hist_success.observe(secs);
    }
    pub fn observe_interarrival_secs(&self, secs: f64) {
        self.interarrival_hist.observe(secs);
    }

    pub fn feed(&self) -> &str {
        &self.feed
    }
    pub fn topic(&self) -> &str {
        &self.topic
    }
}
