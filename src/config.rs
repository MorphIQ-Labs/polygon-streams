use std::{env, time::Duration};

pub struct Config {
    pub sink: String,
    pub cluster: String,
    pub api_key: String,
    pub subscription: String,
    pub channel_capacity: usize,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub read_timeout: Duration,
    pub heartbeat_interval: Option<Duration>,
    pub metrics_addr: String,
    pub ready_min_messages: u64,
    pub ready_delay: Duration,
    pub reconnect_buckets: Vec<f64>,
    pub interarrival_buckets: Vec<f64>,
    pub bp_warn_interval: Duration,
    pub stats_interval: Duration,
    pub ndjson_dest: String,
    pub ndjson_channel_cap: usize,
    pub ndjson_warn_interval: Duration,
    pub ndjson_max_bytes: Option<u64>,
    pub ndjson_rotate_secs: Option<u64>,
    pub ndjson_gzip: bool,
    pub ndjson_reopen_on_hup: bool,
    pub ndjson_flush_every: usize,
    pub ndjson_flush_interval: Duration,
    pub ndjson_include: Option<Vec<String>>, 
    pub ndjson_sample_quotes: u64,
    pub ndjson_split_per_type: bool,

    // ZMQ sink configuration
    pub zmq_endpoint: String,
    pub zmq_bind: bool,
    pub zmq_channel_cap: usize,
    pub zmq_warn_interval: Duration,
    pub zmq_snd_hwm: Option<i32>,
    pub zmq_topic_prefix: String,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let api_key = env::var("POLYGON_API_KEY")
            .map_err(|_| "POLYGON_API_KEY is not set".to_string())?;

        let cluster = env::var("POLYGON_CLUSTER").unwrap_or_else(|_| "futures".to_string());
        let subscription = env::var("SUBSCRIPTION")
            .unwrap_or_else(|_| "T.*".to_string());

        let channel_capacity = env::var("CHANNEL_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024);

        let backoff_initial = env::var("BACKOFF_INITIAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(1));

        let backoff_max = env::var("BACKOFF_MAX_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(60));

        let read_timeout = env::var("READ_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(60));

        let heartbeat_interval = env::var("HEARTBEAT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|secs| if secs == 0 { None } else { Some(Duration::from_secs(secs)) })
            .unwrap_or_else(|| Some(Duration::from_secs(30)));

        let metrics_addr = env::var("METRICS_ADDR").unwrap_or_else(|_| "127.0.0.1:9898".to_string());

        let ready_min_messages = env::var("READY_MIN_MESSAGES").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(1);
        let ready_delay = env::var("READY_DELAY_MS").ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_millis).unwrap_or_else(|| Duration::from_millis(0));

        let reconnect_buckets = parse_buckets(env::var("RECONNECT_BUCKETS").ok().as_deref()).unwrap_or_else(|| vec![0.1,0.5,1.0,2.0,5.0,10.0,30.0,60.0]);
        let interarrival_buckets = parse_buckets(env::var("INTERARRIVAL_BUCKETS").ok().as_deref()).unwrap_or_else(|| vec![0.001,0.005,0.01,0.05,0.1,0.5,1.0,2.0,5.0]);

        let bp_warn_interval = env::var("BP_WARN_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(5));

        let stats_interval = env::var("STATS_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(60));

        let sink = env::var("SINK").unwrap_or_else(|_| "stdout".to_string());

        Ok(Config {
            sink,
            api_key,
            cluster,
            subscription,
            channel_capacity,
            backoff_initial,
            backoff_max,
            read_timeout,
            heartbeat_interval,
            metrics_addr,
            ready_min_messages,
            ready_delay,
            reconnect_buckets,
            interarrival_buckets,
            bp_warn_interval,
            stats_interval,
            ndjson_dest: env::var("NDJSON_DEST").unwrap_or_else(|_| "stdout".to_string()),
            ndjson_channel_cap: env::var("NDJSON_CHANNEL_CAP").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(2048),
            ndjson_warn_interval: env::var("NDJSON_WARN_INTERVAL_SECS").ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_secs).unwrap_or_else(|| Duration::from_secs(5)),
            ndjson_max_bytes: env::var("NDJSON_MAX_BYTES").ok().and_then(|v| v.parse::<u64>().ok()),
            ndjson_rotate_secs: env::var("NDJSON_ROTATE_SECS").ok().and_then(|v| v.parse::<u64>().ok()),
            ndjson_gzip: parse_bool(env::var("NDJSON_GZIP").ok().as_deref()),
            ndjson_reopen_on_hup: !parse_bool(env::var("NDJSON_NO_REOPEN").ok().as_deref()),
            ndjson_flush_every: env::var("NDJSON_FLUSH_EVERY").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(100),
            ndjson_flush_interval: env::var("NDJSON_FLUSH_INTERVAL_MS").ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_millis).unwrap_or_else(|| Duration::from_millis(1000)),
            ndjson_include: env::var("NDJSON_INCLUDE").ok().and_then(|v| {
                let pats: Vec<String> = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                if pats.is_empty() { None } else { Some(pats) }
            }),
            ndjson_sample_quotes: env::var("NDJSON_SAMPLE_QUOTES").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(1),
            ndjson_split_per_type: parse_bool(env::var("NDJSON_SPLIT_PER_TYPE").ok().as_deref()),

            // ZMQ sink
            zmq_endpoint: env::var("ZMQ_ENDPOINT").unwrap_or_else(|_| "tcp://127.0.0.1:5556".to_string()),
            zmq_bind: parse_bool(env::var("ZMQ_BIND").ok().as_deref()),
            zmq_channel_cap: env::var("ZMQ_CHANNEL_CAP").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(4096),
            zmq_warn_interval: env::var("ZMQ_WARN_INTERVAL_SECS").ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_secs).unwrap_or_else(|| Duration::from_secs(5)),
            zmq_snd_hwm: env::var("ZMQ_SND_HWM").ok().and_then(|v| v.parse::<i32>().ok()),
            zmq_topic_prefix: env::var("ZMQ_TOPIC_PREFIX").unwrap_or_else(|_| "".to_string()),
        })
    }
}

fn parse_buckets(src: Option<&str>) -> Option<Vec<f64>> {
    let s = src?;
    let mut out = Vec::new();
    for part in s.split(',') {
        if part.trim().is_empty() { continue; }
        if let Ok(v) = part.trim().parse::<f64>() { out.push(v); }
    }
    if out.is_empty() { None } else { out.sort_by(|a,b| a.partial_cmp(b).unwrap()); Some(out) }
}

fn parse_bool(src: Option<&str>) -> bool {
    match src.map(|s| s.to_ascii_lowercase()) {
        Some(ref s) if s == "1" || s == "true" || s == "yes" || s == "on" => true,
        _ => false,
    }
}
