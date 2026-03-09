#!/usr/bin/env python3
import argparse
import json
import os
import time
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer


def main():
    ap = argparse.ArgumentParser(description="Fallback агрегация 5-мин окон без Flink (для демо).")
    ap.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--in-topic", default=os.getenv("KAFKA_TOPIC_CLICKS", "clicks"))
    ap.add_argument("--out-topic", default=os.getenv("KAFKA_TOPIC_AGG", "aggregated_clicks"))
    ap.add_argument("--window-sec", type=int, default=300)
    ap.add_argument("--poll-sec", type=int, default=5)
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.in_topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
        group_id="fallback-agg",
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
    )

    print(f"Fallback агрегация: {args.in_topic} -> {args.out_topic} bootstrap={args.bootstrap_servers}")

    current_counts = defaultdict(int)
    window_start = int(time.time() // args.window_sec * args.window_sec)

    try:
        while True:
            now = int(time.time())
            if now >= window_start + args.window_sec:
                window_end = window_start + args.window_sec
                for pid, cnt in current_counts.items():
                    msg = {
                        "product_id": int(pid),
                        "window_start_ms": window_start * 1000,
                        "window_end_ms": window_end * 1000,
                        "click_count": int(cnt),
                    }
                    producer.send(args.out_topic, value=msg)
                producer.flush(10)
                current_counts.clear()
                window_start = int(now // args.window_sec * args.window_sec)

            for msg in consumer:
                v = msg.value
                pid = v.get("product_id")
                if pid is not None:
                    current_counts[int(pid)] += 1

            time.sleep(args.poll_sec)
    except KeyboardInterrupt:
        print("Остановлено.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
