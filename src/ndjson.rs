use flate2::write::GzEncoder;
use flate2::Compression;
use futures::future::pending;
use serde::Serialize;
use serde_json::Value;
use std::borrow::Cow;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWriteExt, BufWriter};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::Receiver;
use tokio::sync::Notify;
use tracing::{error, warn};

use crate::metrics::Metrics;

#[derive(Serialize)]
pub struct NdjsonEvent {
    pub ingest_ts: i64,
    pub r#type: Cow<'static, str>,
    pub symbol: String,
    pub topic: String,
    pub feed: String,
    pub ts: i64,
    pub payload: Value,
    pub hostname: String,
    pub app_version: &'static str,
    pub schema_version: u8,
    pub seq: u64,
}

/// Interns common event types to avoid allocations.
///
/// Returns a borrowed static string for common types (T, Q, A, AM, status)
/// and an owned string for any other event types.
pub fn intern_event_type(typ: &str) -> Cow<'static, str> {
    match typ {
        "T" => Cow::Borrowed("T"),
        "Q" => Cow::Borrowed("Q"),
        "A" => Cow::Borrowed("A"),
        "AM" => Cow::Borrowed("AM"),
        "status" => Cow::Borrowed("status"),
        _ => Cow::Owned(typ.to_string()),
    }
}

pub enum NdjsonDest {
    Stdout,
    File(String),
}

impl NdjsonDest {
    pub fn from_str(s: &str) -> Self {
        if s.eq_ignore_ascii_case("stdout") {
            NdjsonDest::Stdout
        } else {
            NdjsonDest::File(s.to_string())
        }
    }
}

pub fn derive_split_paths(path: &str) -> (String, String) {
    let p = std::path::Path::new(path);
    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or(path);
    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
    let parent = p.parent().map(|d| d.to_path_buf()).unwrap_or_default();
    let make = |suffix: &str| -> String {
        let filename = if ext.is_empty() {
            format!("{}.{}", stem, suffix)
        } else {
            format!("{}.{}.{}", stem, suffix, ext)
        };
        parent.join(filename).to_string_lossy().to_string()
    };
    (make("trades"), make("quotes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_paths_with_ext() {
        let (t, q) = derive_split_paths("/var/log/polygon.ndjson");
        assert!(t.ends_with("polygon.trades.ndjson"));
        assert!(q.ends_with("polygon.quotes.ndjson"));
    }
    #[test]
    fn split_paths_no_ext() {
        let (t, q) = derive_split_paths("/var/log/polygon");
        assert!(t.ends_with("polygon.trades"));
        assert!(q.ends_with("polygon.quotes"));
    }
}

pub fn spawn_ndjson_writer(
    mut rx: Receiver<NdjsonEvent>,
    dest: NdjsonDest,
    metrics: Metrics,
    warn_interval: Duration,
    max_bytes: Option<u64>,
    rotate_secs: Option<u64>,
    gzip: bool,
    reopen_on_hup: bool,
    flush_every: usize,
    flush_interval: Duration,
    notify_shutdown: std::sync::Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match dest {
            NdjsonDest::Stdout => {
                let stdout = tokio::io::stdout();
                let mut w = BufWriter::new(stdout);
                writer_loop_stdout(
                    &mut rx,
                    &mut w,
                    &metrics,
                    &notify_shutdown,
                    warn_interval,
                    flush_every,
                    flush_interval,
                )
                .await;
            }
            NdjsonDest::File(path) => {
                writer_loop_file(
                    &mut rx,
                    Path::new(&path),
                    &metrics,
                    &notify_shutdown,
                    warn_interval,
                    max_bytes,
                    rotate_secs,
                    gzip,
                    reopen_on_hup,
                    flush_every,
                    flush_interval,
                )
                .await;
            }
        }
    })
}

async fn writer_loop_stdout<W: AsyncWriteExt + Unpin>(
    rx: &mut Receiver<NdjsonEvent>,
    w: &mut BufWriter<W>,
    metrics: &Metrics,
    notify_shutdown: &std::sync::Arc<Notify>,
    warn_interval: Duration,
    flush_every: usize,
    flush_interval: Duration,
) {
    let mut last_warn = Instant::now() - warn_interval;
    let mut write_errors_since_warn: u64 = 0;
    let mut since_flush: usize = 0;
    let mut last_flush = Instant::now();
    loop {
        tokio::select! {
            _ = notify_shutdown.notified() => { let _ = w.flush().await; break; }
            maybe = rx.recv() => {
                match maybe {
                    Some(ev) => {
                        let buf = match serde_json::to_vec(&ev) { Ok(mut v) => { v.push(b'\n'); v }, Err(_) => { write_errors_since_warn+=1; if last_warn.elapsed()>=warn_interval{ warn!(count = write_errors_since_warn, "ndjson_serialize_error"); write_errors_since_warn=0; last_warn=Instant::now(); } continue; } };
                        if let Err(_) = w.write_all(&buf).await { write_errors_since_warn+=1; if last_warn.elapsed()>=warn_interval{ warn!(count = write_errors_since_warn, "ndjson_write_error"); write_errors_since_warn=0; last_warn=Instant::now(); } } else { metrics.inc_ndjson_written(); since_flush+=1; }
                        if since_flush >= flush_every || last_flush.elapsed() >= flush_interval { let _ = w.flush().await; since_flush = 0; last_flush = Instant::now(); }
                    }
                    None => { let _ = w.flush().await; break; }
                }
            }
        }
    }
}

async fn writer_loop_file(
    rx: &mut Receiver<NdjsonEvent>,
    path: &Path,
    metrics: &Metrics,
    notify_shutdown: &std::sync::Arc<Notify>,
    warn_interval: Duration,
    max_bytes: Option<u64>,
    rotate_secs: Option<u64>,
    gzip: bool,
    reopen_on_hup: bool,
    flush_every: usize,
    flush_interval: Duration,
) {
    let f = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!(error=%e, "ndjson_open_failed");
            return;
        }
    };
    let mut w = BufWriter::new(f);
    let mut last_warn = Instant::now() - warn_interval;
    let mut write_errors_since_warn: u64 = 0;
    let mut since_flush: usize = 0;
    let mut last_flush = Instant::now();
    let mut written_bytes: u64 = 0;
    let mut last_rotate = Instant::now();
    #[cfg(unix)]
    let mut hup_stream = if reopen_on_hup {
        Some(signal(SignalKind::hangup()).expect("hup"))
    } else {
        None
    };
    #[cfg(not(unix))]
    let mut hup_stream: Option<()> = None;
    loop {
        tokio::select! {
            _ = notify_shutdown.notified() => { let _ = w.flush().await; break; }
            _ = wait_for_hup(&mut hup_stream, reopen_on_hup) => {
                let _ = w.flush().await;
                match tokio::fs::OpenOptions::new().create(true).append(true).open(path).await { Ok(f)=>{ w = BufWriter::new(f); written_bytes = 0; last_rotate = Instant::now(); }, Err(e)=> error!(error=%e, "ndjson_reopen_failed") }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(ev) => {
                        let buf = match serde_json::to_vec(&ev) { Ok(mut v) => { v.push(b'\n'); v }, Err(_) => { write_errors_since_warn+=1; if last_warn.elapsed()>=warn_interval{ warn!(count = write_errors_since_warn, "ndjson_serialize_error"); write_errors_since_warn=0; last_warn=Instant::now(); } continue; } };
                        if let Err(_) = w.write_all(&buf).await { write_errors_since_warn+=1; if last_warn.elapsed()>=warn_interval{ warn!(count = write_errors_since_warn, "ndjson_write_error"); write_errors_since_warn=0; last_warn=Instant::now(); } } else { metrics.inc_ndjson_written(); since_flush+=1; written_bytes += buf.len() as u64; }
                        if since_flush >= flush_every || last_flush.elapsed() >= flush_interval { let _ = w.flush().await; since_flush = 0; last_flush = Instant::now(); }
                        let need_rotate = max_bytes.map(|m| written_bytes>=m).unwrap_or(false) || rotate_secs.map(|s| last_rotate.elapsed() >= Duration::from_secs(s)).unwrap_or(false);
                        if need_rotate { let _ = w.flush().await; rotate_file(path, gzip).await; match tokio::fs::OpenOptions::new().create(true).append(true).open(path).await { Ok(f)=>{ w = BufWriter::new(f); written_bytes = 0; last_rotate = Instant::now(); }, Err(e)=> error!(error=%e, "ndjson_reopen_failed") } }
                    }
                    None => { let _ = w.flush().await; break; }
                }
            }
        }
    }
}

#[cfg(unix)]
async fn wait_for_hup(hup_stream: &mut Option<tokio::signal::unix::Signal>, enabled: bool) {
    if enabled {
        if let Some(s) = hup_stream.as_mut() {
            let _ = s.recv().await;
        }
    } else {
        pending::<()>().await;
    }
}

#[cfg(not(unix))]
async fn wait_for_hup(_hup_stream: &mut Option<()>, _enabled: bool) {
    pending::<()>().await;
}

async fn rotate_file(path: &Path, gzip: bool) {
    let ts = now_millis();
    let rotated = path.with_extension(format!("{}.log", ts));
    let _ = tokio::fs::rename(path, &rotated).await;
    if gzip {
        let rotated_gz = rotated.with_extension(format!("{}.gz", ts));
        let rotated_clone = rotated.clone();
        let rotated_gz_clone = rotated_gz.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let input = std::fs::File::open(&rotated_clone).map_err(|_| ())?;
            let mut reader = std::io::BufReader::new(input);
            let output = std::fs::File::create(&rotated_gz_clone).map_err(|_| ())?;
            let mut encoder = GzEncoder::new(output, Compression::default());
            std::io::copy(&mut reader, &mut encoder).map_err(|_| ())?;
            encoder.finish().map_err(|_| ())?;
            let _ = std::fs::remove_file(&rotated_clone);
            Ok::<(), ()>(())
        })
        .await;
    }
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
