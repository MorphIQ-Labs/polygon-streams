// src/model.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct RequestMessage<'a> {
    pub action: &'a str,
    pub params: &'a str,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StatusMessage {
    pub status: String,
    pub message: Option<String>,
    // Add other fields as necessary
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct StockTradeMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "x")]
    pub(crate) exchange_id: i16,
    #[serde(rename = "i")]
    pub(crate) trade_id: String,
    #[serde(rename = "z")]
    pub(crate) tape_id: i8,
    #[serde(rename = "p")]
    pub(crate) price: f64,
    #[serde(rename = "s")]
    pub(crate) size: i32,
    #[serde(rename = "c")]
    pub(crate) trade_conditions: Option<Vec<i16>>,
    #[serde(rename = "t")]
    pub(crate) timestamp: i64,
    #[serde(rename = "trfi")]
    pub(crate) trf_id: Option<i16>,
    #[serde(rename = "trft")]
    pub(crate) trf_ts: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuturesTradeMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "p")]
    pub(crate) price: f64,
    #[serde(rename = "s")]
    pub(crate) size: i32,
    #[serde(rename = "c")]
    pub(crate) trade_conditions: Option<Vec<i16>>,
    #[serde(rename = "t")]
    pub(crate) timestamp: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct StockAggregateMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "v")]
    pub(crate) volume_tick: i32,
    #[serde(rename = "av")]
    pub(crate) volume_day: i32,
    #[serde(rename = "op")]
    pub(crate) open_price: f64,
    #[serde(rename = "vw")]
    pub(crate) tick_vwap: f64,
    #[serde(rename = "o")]
    pub(crate) tick_open: f64,
    #[serde(rename = "c")]
    pub(crate) tick_close: f64,
    #[serde(rename = "h")]
    pub(crate) tick_high: f64,
    #[serde(rename = "l")]
    pub(crate) tick_low: f64,
    #[serde(rename = "a")]
    pub(crate) vwap: f64,
    #[serde(rename = "z")]
    pub(crate) avg_trade_size: i32,
    #[serde(rename = "s")]
    pub(crate) start_ts: i64,
    #[serde(rename = "e")]
    pub(crate) end_ts: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct StockQuoteMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "bx")]
    pub(crate) bid_exchange_id: i16,
    #[serde(rename = "bp")]
    pub(crate) bid_price: f64,
    #[serde(rename = "bs")]
    pub(crate) bid_size: i16,
    #[serde(rename = "ax")]
    pub(crate) ask_exchange_id: i16,
    #[serde(rename = "ap")]
    pub(crate) ask_price: f64,
    #[serde(rename = "as")]
    pub(crate) ask_size: i16,
    #[serde(rename = "c")]
    pub(crate) trade_conditions: Option<i16>,
    #[serde(rename = "i")]
    pub(crate) indicators: Option<Vec<i16>>,
    #[serde(rename = "t")]
    pub(crate) sip_ts: i64,
    #[serde(rename = "q")]
    pub(crate) sequence: i64,
    #[serde(rename = "z")]
    pub(crate) tape: i8,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuturesQuoteMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "bx")]
    pub(crate) bid_exchange_id: i16,
    #[serde(rename = "bp")]
    pub(crate) bid_price: f64,
    #[serde(rename = "bs")]
    pub(crate) bid_size: i16,
    #[serde(rename = "ax")]
    pub(crate) ask_exchange_id: i16,
    #[serde(rename = "ap")]
    pub(crate) ask_price: f64,
    #[serde(rename = "as")]
    pub(crate) ask_size: i16,
    #[serde(rename = "c")]
    pub(crate) trade_conditions: Option<i16>,
    #[serde(rename = "i")]
    pub(crate) indicators: Option<Vec<i16>>,
    #[serde(rename = "t")]
    pub(crate) sip_ts: i64,
    #[serde(rename = "q")]
    pub(crate) sequence: i64,
    #[serde(rename = "z")]
    pub(crate) tape: i8,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(tag = "ev")]
pub enum PolygonStockMessage {
    #[serde(rename = "status")]
    Status(StatusMessage),
    #[serde(rename = "T")]
    Trade(StockTradeMessage),
    #[serde(rename = "A")]
    AggregateSecond(StockAggregateMessage),
    #[serde(rename = "AM")]
    AggregateMinute(StockAggregateMessage),
    #[serde(rename = "Q")]
    Quote(StockQuoteMessage),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "ev")]
pub enum PolygonFuturesMessage {
    #[serde(rename = "status")]
    Status(StatusMessage),
    #[serde(rename = "T")]
    Trade(FuturesTradeMessage),
    #[serde(rename = "Q")]
    Quote(FuturesQuoteMessage),
}

// Options feed (minimal subset)
#[derive(Debug, Deserialize, Serialize)]
pub struct OptionsTradeMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "p")]
    pub(crate) price: f64,
    #[serde(rename = "s")]
    pub(crate) size: f64,
    #[serde(rename = "t")]
    pub(crate) timestamp: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OptionsQuoteMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "bp")]
    pub(crate) bid_price: f64,
    #[serde(rename = "bs")]
    pub(crate) bid_size: f64,
    #[serde(rename = "ap")]
    pub(crate) ask_price: f64,
    #[serde(rename = "as")]
    pub(crate) ask_size: f64,
    #[serde(rename = "t")]
    pub(crate) ts: i64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "ev")]
pub enum PolygonOptionsMessage {
    #[serde(rename = "status")]
    Status(StatusMessage),
    #[serde(rename = "T")]
    Trade(OptionsTradeMessage),
    #[serde(rename = "Q")]
    Quote(OptionsQuoteMessage),
}

// Crypto feed (minimal subset)
#[derive(Debug, Deserialize, Serialize)]
pub struct CryptoTradeMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "p")]
    pub(crate) price: f64,
    #[serde(rename = "s")]
    pub(crate) size: f64,
    #[serde(rename = "t")]
    pub(crate) timestamp: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CryptoQuoteMessage {
    #[serde(rename = "sym")]
    pub(crate) symbol: String,
    #[serde(rename = "bp")]
    pub(crate) bid_price: f64,
    #[serde(rename = "bs")]
    pub(crate) bid_size: f64,
    #[serde(rename = "ap")]
    pub(crate) ask_price: f64,
    #[serde(rename = "as")]
    pub(crate) ask_size: f64,
    #[serde(rename = "t")]
    pub(crate) ts: i64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "ev")]
pub enum PolygonCryptoMessage {
    #[serde(rename = "status")]
    Status(StatusMessage),
    #[serde(rename = "T")]
    Trade(CryptoTradeMessage),
    #[serde(rename = "Q")]
    Quote(CryptoQuoteMessage),
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_status_message() {
        let json_data = r#"
        [
            {
                "ev": "status",
                "status": "auth_success",
                "message": "authenticated"
            }
        ]
        "#;

        let messages: Vec<PolygonStockMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonStockMessage::Status(status) => {
                assert_eq!(status.status, "auth_success");
                assert_eq!(status.message.as_deref(), Some("authenticated"));
            }
            _ => panic!("Expected a StatusMessage"),
        }
    }

    #[test]
    fn test_deserialize_trade_message() {
        let json_data = r#"
        [
            {
                "ev": "T",
                "sym": "AAPL",
                "p": 150.25,
                "s": 100,
                "x": 4,
                "t": 1730836779020,
                "q": 974217,
                "z": 1,
                "i": "4"
            }
        ]
        "#;

        let messages: Vec<PolygonStockMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonStockMessage::Trade(trade) => {
                assert_eq!(trade.symbol, "AAPL");
                assert_eq!(trade.price, 150.25);
                assert_eq!(trade.size, 100);
                assert_eq!(trade.exchange_id, 4);
                assert_eq!(trade.timestamp, 1730836779020);
                assert_eq!(trade.tape_id, 1);
                assert_eq!(trade.trade_id, "4");
            }
            _ => panic!("Expected a TradeMessage"),
        }
    }

    #[test]
    fn test_deserialize_futures_trade_message() {
        let json_data = r#"
        [
            {
                "ev": "T",
                "sym": "ESZ4",
                "p": 4935.25,
                "s": 3,
                "c": [0, 12],
                "t": 1730836779020
            }
        ]
        "#;

        let messages: Vec<PolygonFuturesMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonFuturesMessage::Trade(trade) => {
                assert_eq!(trade.symbol, "ESZ4");
                assert_eq!(trade.price, 4935.25);
                assert_eq!(trade.size, 3);
                assert_eq!(trade.trade_conditions.as_ref().unwrap(), &vec![0, 12]);
                assert_eq!(trade.timestamp, 1730836779020);
            }
            _ => panic!("Expected a FuturesTradeMessage"),
        }
    }

    #[test]
    fn test_deserialize_futures_quote_message() {
        let json_data = r#"
        [
            {
                "ev": "Q",
                "sym": "ESZ4",
                "bx": 4,
                "bp": 4935.25,
                "bs": 5,
                "ax": 7,
                "ap": 4935.50,
                "as": 8,
                "c": 1,
                "i": [0],
                "t": 1730836779020,
                "q": 987654,
                "z": 1
            }
        ]
        "#;

        let messages: Vec<PolygonFuturesMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonFuturesMessage::Quote(quote) => {
                assert_eq!(quote.symbol, "ESZ4");
                assert_eq!(quote.bid_exchange_id, 4);
                assert_eq!(quote.bid_price, 4935.25);
                assert_eq!(quote.bid_size, 5);
                assert_eq!(quote.ask_exchange_id, 7);
                assert_eq!(quote.ask_price, 4935.50);
                assert_eq!(quote.ask_size, 8);
                assert_eq!(quote.trade_conditions, Some(1));
                assert_eq!(quote.indicators.as_ref().unwrap(), &vec![0]);
                assert_eq!(quote.sip_ts, 1730836779020);
                assert_eq!(quote.sequence, 987654);
                assert_eq!(quote.tape, 1);
            }
            _ => panic!("Expected a FuturesQuoteMessage"),
        }
    }

    #[test]
    fn test_deserialize_aggregate_second_message() {
        let json_data = r#"
        [
            {
                "ev": "A",
                "sym": "AAPL",
                "v": 1000,
                "av": 5000,
                "op": 150.0,
                "vw": 150.25,
                "o": 150.1,
                "c": 150.5,
                "h": 150.6,
                "l": 149.9,
                "a": 150.3,
                "z": 100,
                "s": 1616688000000,
                "e": 1616688060000
            }
        ]
        "#;

        let messages: Vec<PolygonStockMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonStockMessage::AggregateSecond(agg) => {
                assert_eq!(agg.symbol, "AAPL");
                assert_eq!(agg.volume_tick, 1000);
                assert_eq!(agg.volume_day, 5000);
                assert_eq!(agg.open_price, 150.0);
                assert_eq!(agg.tick_vwap, 150.25);
                assert_eq!(agg.tick_open, 150.1);
                assert_eq!(agg.tick_close, 150.5);
                assert_eq!(agg.tick_high, 150.6);
                assert_eq!(agg.tick_low, 149.9);
                assert_eq!(agg.vwap, 150.3);
                assert_eq!(agg.avg_trade_size, 100);
                assert_eq!(agg.start_ts, 1616688000000);
                assert_eq!(agg.end_ts, 1616688060000);
            }
            _ => panic!("Expected an AggregateSecond message"),
        }
    }

    #[test]
    fn test_deserialize_aggregate_minute_message() {
        let json_data = r#"
        [
            {
                "ev": "AM",
                "sym": "AAPL",
                "v": 6000,
                "av": 20000,
                "op": 149.5,
                "vw": 150.0,
                "o": 149.8,
                "c": 150.2,
                "h": 150.4,
                "l": 149.7,
                "a": 150.1,
                "z": 200,
                "s": 1616688000000,
                "e": 1616688600000
            }
        ]
        "#;

        let messages: Vec<PolygonStockMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonStockMessage::AggregateMinute(agg) => {
                assert_eq!(agg.symbol, "AAPL");
                assert_eq!(agg.volume_tick, 6000);
                assert_eq!(agg.volume_day, 20000);
                assert_eq!(agg.open_price, 149.5);
                assert_eq!(agg.tick_vwap, 150.0);
                assert_eq!(agg.tick_open, 149.8);
                assert_eq!(agg.tick_close, 150.2);
                assert_eq!(agg.tick_high, 150.4);
                assert_eq!(agg.tick_low, 149.7);
                assert_eq!(agg.vwap, 150.1);
                assert_eq!(agg.avg_trade_size, 200);
                assert_eq!(agg.start_ts, 1616688000000);
                assert_eq!(agg.end_ts, 1616688600000);
            }
            _ => panic!("Expected an AggregateMinute message"),
        }
    }

    #[test]
    fn test_deserialize_quote_message() {
        let json_data = r#"
        [
            {
                "ev": "Q",
                "sym": "AAPL",
                "bx": 4,
                "bp": 150.25,
                "bs": 100,
                "ax": 7,
                "ap": 150.30,
                "as": 200,
                "c": 1,
                "i": [0, 1],
                "t": 1616688000000,
                "q": 123456789,
                "z": 1
            }
        ]
        "#;

        let messages: Vec<PolygonStockMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(messages.len(), 1);

        match &messages[0] {
            PolygonStockMessage::Quote(quote) => {
                assert_eq!(quote.symbol, "AAPL");
                assert_eq!(quote.bid_exchange_id, 4);
                assert_eq!(quote.bid_price, 150.25);
                assert_eq!(quote.bid_size, 100);
                assert_eq!(quote.ask_exchange_id, 7);
                assert_eq!(quote.ask_price, 150.30);
                assert_eq!(quote.ask_size, 200);
                assert_eq!(quote.trade_conditions, Some(1));
                assert_eq!(quote.indicators.as_ref().unwrap(), &vec![0, 1]);
                assert_eq!(quote.sip_ts, 1616688000000);
                assert_eq!(quote.sequence, 123456789);
                assert_eq!(quote.tape, 1);
            }
            _ => panic!("Expected a QuoteMessage"),
        }
    }

    #[test]
    fn test_serialize_request_message() {
        let request = RequestMessage {
            action: "subscribe",
            params: "Q.AAPL",
        };

        let json_string = serde_json::to_string(&request).unwrap();
        assert_eq!(json_string, r#"{"action":"subscribe","params":"Q.AAPL"}"#);
    }

    #[test]
    fn test_deserialize_options_trade_quote() {
        let json_data = r#"[
          {"ev":"T","sym":"AAPL240118C00180000","p":1.23,"s":10,"t":1730836779020},
          {"ev":"Q","sym":"AAPL240118C00180000","bp":1.2,"bs":5,"ap":1.3,"as":7,"t":1730836779021}
        ]"#;
        let msgs: Vec<PolygonOptionsMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(msgs.len(), 2);
        match &msgs[0] {
            PolygonOptionsMessage::Trade(t) => {
                assert_eq!(t.symbol, "AAPL240118C00180000");
                assert_eq!(t.size as i64, 10);
            }
            _ => panic!("expected trade"),
        }
        match &msgs[1] {
            PolygonOptionsMessage::Quote(q) => {
                assert_eq!(q.symbol, "AAPL240118C00180000");
                assert!((q.bid_price - 1.2).abs() < 1e-9);
            }
            _ => panic!("expected quote"),
        }
    }

    #[test]
    fn test_deserialize_crypto_trade_quote() {
        let json_data = r#"[
          {"ev":"T","sym":"BTC-USD","p":40000.5,"s":0.01,"t":1730836779020},
          {"ev":"Q","sym":"BTC-USD","bp":40000.0,"bs":0.5,"ap":40001.0,"as":0.25,"t":1730836779021}
        ]"#;
        let msgs: Vec<PolygonCryptoMessage> = serde_json::from_str(json_data).unwrap();
        assert_eq!(msgs.len(), 2);
        match &msgs[0] {
            PolygonCryptoMessage::Trade(t) => {
                assert_eq!(t.symbol, "BTC-USD");
                assert!((t.price - 40000.5).abs() < 1e-9);
            }
            _ => panic!("expected trade"),
        }
        match &msgs[1] {
            PolygonCryptoMessage::Quote(q) => {
                assert_eq!(q.symbol, "BTC-USD");
                assert_eq!(q.bid_price as i64, 40000);
            }
            _ => panic!("expected quote"),
        }
    }
}
