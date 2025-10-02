use super::{ClusterHandler, NormalizedEvent};
use serde_json::Value;

pub enum ClusterKind {
    Named(String),
}

pub struct GenericHandler {
    url: String,
}

impl GenericHandler {
    pub fn new(kind: ClusterKind) -> Self {
        match kind {
            ClusterKind::Named(n) => Self {
                url: format!("wss://socket.polygon.io/{}", n),
            },
        }
    }
}

impl ClusterHandler for GenericHandler {
    fn ws_url(&self) -> &str {
        &self.url
    }
    fn default_subscription(&self) -> &str {
        "T.*"
    }

    fn normalize_messages(
        &self,
        text: &str,
    ) -> Result<Vec<NormalizedEvent>, Box<dyn std::error::Error>> {
        let vals: Vec<Value> = serde_json::from_str(text)?;
        let mut out = Vec::with_capacity(vals.len());
        for v in vals.into_iter() {
            let ev = v
                .get("ev")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let symbol = v.get("sym").and_then(|x| x.as_str()).map(|s| s.to_string());
            let ts = v
                .get("ts")
                .and_then(|x| x.as_i64())
                .or_else(|| v.get("t").and_then(|x| x.as_i64()))
                .or_else(|| v.get("sip_ts").and_then(|x| x.as_i64()));
            out.push(NormalizedEvent {
                ev,
                symbol,
                ts,
                payload: v,
            });
        }
        Ok(out)
    }
}
