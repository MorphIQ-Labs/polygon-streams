use super::{ClusterHandler, NormalizedEvent};
use crate::model::PolygonStockMessage;
use serde_json;

pub struct StocksHandler;

impl ClusterHandler for StocksHandler {
    fn ws_url(&self) -> &str {
        "wss://socket.polygon.io/stocks"
    }
    fn default_subscription(&self) -> &str {
        "T.*"
    }

    fn normalize_messages(
        &self,
        text: &str,
    ) -> Result<Vec<NormalizedEvent>, Box<dyn std::error::Error>> {
        let msgs: Vec<PolygonStockMessage> = serde_json::from_str(text)?;
        let mut out = Vec::with_capacity(msgs.len());
        for m in msgs {
            match m {
                PolygonStockMessage::Status(status) => out.push(NormalizedEvent {
                    ev: "status".to_string(),
                    symbol: None,
                    ts: None,
                    payload: serde_json::to_value(status)?,
                }),
                PolygonStockMessage::Trade(tr) => out.push(NormalizedEvent {
                    ev: "T".to_string(),
                    symbol: Some(tr.symbol.clone()),
                    ts: Some(tr.timestamp),
                    payload: serde_json::to_value(tr)?,
                }),
                PolygonStockMessage::AggregateSecond(ag) => out.push(NormalizedEvent {
                    ev: "A".to_string(),
                    symbol: Some(ag.symbol.clone()),
                    ts: Some(ag.end_ts),
                    payload: serde_json::to_value(ag)?,
                }),
                PolygonStockMessage::AggregateMinute(ag) => out.push(NormalizedEvent {
                    ev: "AM".to_string(),
                    symbol: Some(ag.symbol.clone()),
                    ts: Some(ag.end_ts),
                    payload: serde_json::to_value(ag)?,
                }),
                PolygonStockMessage::Quote(q) => out.push(NormalizedEvent {
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
