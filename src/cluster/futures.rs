use super::{ClusterHandler, NormalizedEvent};
use crate::model::PolygonFuturesMessage;
use serde_json;

pub struct FuturesHandler;

impl ClusterHandler for FuturesHandler {
    fn ws_url(&self) -> &str {
        "wss://socket.polygon.io/futures"
    }
    fn default_subscription(&self) -> &str {
        "T.*"
    }

    fn normalize_messages(
        &self,
        text: &str,
    ) -> Result<Vec<NormalizedEvent>, Box<dyn std::error::Error>> {
        let msgs: Vec<PolygonFuturesMessage> = serde_json::from_str(text)?;
        let mut out = Vec::with_capacity(msgs.len());
        for m in msgs {
            match m {
                PolygonFuturesMessage::Status(status) => out.push(NormalizedEvent {
                    ev: "status".to_string(),
                    symbol: None,
                    ts: None,
                    payload: serde_json::to_value(status)?,
                }),
                PolygonFuturesMessage::Trade(tr) => out.push(NormalizedEvent {
                    ev: "T".to_string(),
                    symbol: Some(tr.symbol.clone()),
                    ts: Some(tr.timestamp),
                    payload: serde_json::to_value(tr)?,
                }),
                PolygonFuturesMessage::Quote(q) => out.push(NormalizedEvent {
                    ev: "Q".to_string(),
                    symbol: Some(q.symbol.clone()),
                    ts: Some(q.sip_ts),
                    payload: serde_json::to_value(q)?,
                }),
            }
        }
        Ok(out)
    }
}
