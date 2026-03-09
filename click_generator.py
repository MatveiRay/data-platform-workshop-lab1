#!/usr/bin/env python3
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def build_products(n: int = 100, n_cats: int = 5):
    return [{"id": i, "category": f"cat_{i % n_cats}"} for i in range(1, n + 1)]


def generate_click(products):
    p = random.choice(products)
    ts_ms = now_ms()
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "product_id": int(p["id"]),
        "category": p["category"],
        "event_ts_ms": ts_ms,
        "event_ts": iso_utc(ts_ms),
    }


def main():
    ap = argparse.ArgumentParser(description="Генератор тестовых кликов в Kafka.")
    ap.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_CLICKS", "clicks"))
    ap.add_argument("--rate", type=float, default=5.0, help="среднее число событий в секунду")
    ap.add_argument("--products", type=int, default=100)
    args = ap.parse_args()

    products = build_products(n=args.products)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        api_version=(3, 6, 0),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
    )
    
    print(f"Генератор запущен. bootstrap={args.bootstrap_servers} topic={args.topic} rate={args.rate}/s")
    try:
        while True:
            click = generate_click(products)
            producer.send(args.topic, value=click)
            print(click)
            time.sleep(random.expovariate(args.rate))
    except KeyboardInterrupt:
        print("Остановлено пользователем.")
    finally:
        producer.flush(10)
        producer.close()


if __name__ == "__main__":
    main()
