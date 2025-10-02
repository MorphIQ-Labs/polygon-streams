use serde_json::Value;

#[derive(Debug, Clone)]
pub struct NormalizedEvent {
    pub ev: String,         // e.g., "T", "Q", "A", "AM", "status"
    pub symbol: Option<String>,
    pub ts: Option<i64>,
    pub payload: Value,
}

pub trait ClusterHandler: Send + Sync {
    fn ws_url(&self) -> &str;
    fn default_subscription(&self) -> &str { "T.*" }
    fn normalize_messages(&self, text: &str) -> Result<Vec<NormalizedEvent>, Box<dyn std::error::Error>>;
}

mod stocks;
mod futures;
mod options;
mod crypto;
mod generic;

pub use stocks::StocksHandler;
pub use futures::FuturesHandler;
pub use options::OptionsHandler;
pub use crypto::CryptoHandler;
pub use generic::{GenericHandler, ClusterKind};

pub fn build_handler(cluster: &str) -> Box<dyn ClusterHandler> {
    match cluster.to_ascii_lowercase().as_str() {
        "stocks" => Box::new(StocksHandler),
        "futures" => Box::new(FuturesHandler),
        "options" => Box::new(OptionsHandler),
        "crypto" => Box::new(CryptoHandler),
        other => {
            // Fallback to generic with provided name
            Box::new(GenericHandler::new(ClusterKind::Named(other.to_string())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_maps_correctly() {
        assert_eq!(build_handler("stocks").ws_url(), "wss://socket.polygon.io/stocks");
        assert_eq!(build_handler("futures").ws_url(), "wss://socket.polygon.io/futures");
        assert_eq!(build_handler("options").ws_url(), "wss://socket.polygon.io/options");
        assert_eq!(build_handler("crypto").ws_url(), "wss://socket.polygon.io/crypto");
    }

    #[test]
    fn generic_named_cluster_maps_url() {
        let h = build_handler("indices");
        assert!(h.ws_url().ends_with("/indices"));
    }

    #[test]
    fn normalize_futures_trade_quote() {
        let h = super::FuturesHandler;
        let json = r#"[
          {"ev":"T","sym":"ESZ4","p":4935.25,"s":3,"t":1730836779020},
          {"ev":"Q","sym":"ESZ4","bx":4,"bp":4935.25,"bs":5,"ax":7,"ap":4935.50,"as":8,"i":[0],"t":1730836779021,"q":123,"z":1}
        ]"#;
        let events = h.normalize_messages(json).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].ev, "T");
        assert_eq!(events[0].symbol.as_deref(), Some("ESZ4"));
        assert_eq!(events[1].ev, "Q");
    }

    #[test]
    fn normalize_stocks_trade_quote() {
        let h = super::StocksHandler;
        let json = r#"[
          {"ev":"T","sym":"AAPL","p":150.25,"s":100,"x":4,"t":1730836779020,"q":974217,"z":1,"i":"4"},
          {"ev":"Q","sym":"AAPL","bx":4,"bp":150.25,"bs":100,"ax":7,"ap":150.30,"as":200,"t":1616688000000,"q":123456789,"z":1}
        ]"#;
        let events = h.normalize_messages(json).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].ev, "T");
        assert_eq!(events[0].symbol.as_deref(), Some("AAPL"));
        assert_eq!(events[1].ev, "Q");
    }
}
