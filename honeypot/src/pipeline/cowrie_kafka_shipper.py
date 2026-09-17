"""Tail Cowrie JSON logs and publish lines to Kafka (cowrie.events)."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def main() -> int:
    log_file = Path(os.environ.get("COWRIE_LOG_FILE", "/cowrie/cowrie/log/cowrie.json"))
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_COWRIE_TOPIC", "cowrie.events")
    start_at_end = os.environ.get("COWRIE_SHIPPER_START_AT_END", "1") not in ("0", "false", "False")

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Installing kafka-python...", flush=True)
        import subprocess

        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "kafka-python>=2.0.2"])
        from kafka import KafkaProducer

    print(f"Waiting for log file: {log_file}", flush=True)
    while not log_file.is_file():
        time.sleep(2)

    producer = None
    for attempt in range(60):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap.split(","),
                value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else str(v).encode("utf-8"),
                linger_ms=50,
            )
            break
        except Exception as exc:
            print(f"Kafka not ready ({exc}); retry {attempt+1}/60", flush=True)
            time.sleep(2)
    if producer is None:
        print("Could not connect to Kafka", flush=True)
        return 1

    print(f"Shipping {log_file} → {topic} @ {bootstrap}", flush=True)
    with log_file.open("r", encoding="utf-8", errors="replace") as fh:
        if start_at_end:
            fh.seek(0, os.SEEK_END)
        while True:
            line = fh.readline()
            if not line:
                time.sleep(0.5)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)  # validate JSON
            except Exception:
                continue
            producer.send(topic, line.encode("utf-8"))
            producer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
