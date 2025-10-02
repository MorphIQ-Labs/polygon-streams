use super::{ClusterHandler, NormalizedEvent};
use crate::model::PolygonOptionsMessage;
use serde_json;

pub struct OptionsHandler;

impl ClusterHandler for OptionsHandler {
    fn ws_url(&self) -> &str {
        "wss://socket.polygon.io/options"
    }
    fn default_subscription(&self) -> &str {
        "T.*"
    }

    fn normalize_messages(
        &self,
        text: &str,
    ) -> Result<Vec<NormalizedEvent>, Box<dyn std::error::Error>> {
        let msgs: Vec<PolygonOptionsMessage> = serde_json::from_str(text)?;
        let mut out = Vec::with_capacity(msgs.len());
        for m in msgs {
            match m {
                PolygonOptionsMessage::Status(status) => out.push(NormalizedEvent {
                    ev: "status".to_string(),
                    symbol: None,
                    ts: None,
                    payload: serde_json::to_value(status)?,
                }),
                PolygonOptionsMessage::Trade(tr) => out.push(NormalizedEvent {
                    ev: "T".to_string(),
                    symbol: Some(tr.symbol.clone()),
                    ts: Some(tr.timestamp),
                    payload: serde_json::to_value(tr)?,
                }),
                PolygonOptionsMessage::Quote(q) => out.push(NormalizedEvent {
                    ev: "Q".to_string(),
                    symbol: Some(q.symbol.clone()),
                    ts: Some(q.ts),
                    payload: serde_json::to_value(q)?,
                }),
            }
        }
        Ok(out)
    }
}
