// Simple ZMQ subscriber example
// Build and run:
//   cargo run --release --features zmq-sink --example zmq_sub
// Env:
//   ZMQ_SUB_ENDPOINT=tcp://127.0.0.1:5556
//   ZMQ_TOPIC="" (empty = all topics; e.g., "T:" or "T:ES" to filter)

use std::env;

fn main() {
    let endpoint = env::var("ZMQ_SUB_ENDPOINT").unwrap_or_else(|_| "tcp://127.0.0.1:5556".to_string());
    let topic = env::var("ZMQ_TOPIC").unwrap_or_else(|_| String::new());

    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::SocketType::SUB).expect("sub socket");
    sock.connect(&endpoint).expect("connect");
    sock.set_subscribe(topic.as_bytes()).expect("subscribe");
    eprintln!("Subscribed to {} with topic prefix {:?}", endpoint, topic);

    loop {
        // Multipart frames: [topic, payload]
        match sock.recv_multipart(0) {
            Ok(frames) => {
                if frames.len() == 2 {
                    let topic = String::from_utf8_lossy(&frames[0]);
                    let payload = String::from_utf8_lossy(&frames[1]);
                    println!("{} {}", topic, payload.trim_end());
                } else {
                    println!("recv {:?}", frames);
                }
            }
            Err(e) => {
                eprintln!("recv error: {}", e);
                break;
            }
        }
    }
}

