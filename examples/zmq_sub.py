#!/usr/bin/env python3
"""
Simple ZeroMQ SUB example for polygon-rs ZMQ PUB sink.

Usage:
  pip install pyzmq
  ZMQ_SUB_ENDPOINT=tcp://127.0.0.1:5556 ZMQ_TOPIC="" python3 examples/zmq_sub.py

Env:
  ZMQ_SUB_ENDPOINT  PUB endpoint to connect to (default: tcp://127.0.0.1:5556)
  ZMQ_TOPIC         Subscription prefix (empty = all; e.g., "T:" or "T:ES")
"""

import os
import sys
import zmq  # type: ignore


def main() -> int:
    endpoint = os.getenv("ZMQ_SUB_ENDPOINT", "tcp://127.0.0.1:5556")
    topic = os.getenv("ZMQ_TOPIC", "").encode()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt(zmq.SUBSCRIBE, topic)
    print(f"Subscribed to {endpoint} with topic prefix {topic.decode(errors='ignore')!r}", file=sys.stderr)

    try:
        while True:
            # Expect multipart: [topic, payload]
            frames = sock.recv_multipart()
            if len(frames) != 2:
                print(f"recv {frames}")
                continue
            t = frames[0].decode("utf-8", errors="replace")
            payload = frames[1].decode("utf-8", errors="replace").rstrip("\n")
            print(f"{t} {payload}")
    except KeyboardInterrupt:
        return 0
    finally:
        sock.close(0)
        ctx.term()


if __name__ == "__main__":
    raise SystemExit(main())

