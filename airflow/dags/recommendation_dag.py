import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.redis.hooks.redis import RedisHook
from kafka import KafkaConsumer, TopicPartition
import duckdb


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_AGG = _env("KAFKA_TOPIC_AGG", "aggregated_clicks")

REDIS_CONN_ID = _env("REDIS_CONN_ID", "redis_default")

ORDERS_CSV_PATH = _env("ORDERS_CSV_PATH", "/opt/airflow/orders.csv")
DUCKDB_PATH = _env("DUCKDB_PATH", "/opt/airflow/recommendations.duckdb")

WEIGHT_CLICKS = float(_env("WEIGHT_CLICKS", "0.7"))
WEIGHT_ORDERS = float(_env("WEIGHT_ORDERS", "0.3"))

DEFAULT_ARGS = {
    "owner": "student",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def read_latest_window_aggregates(**context) -> None:
    """
    Читает сообщения из Kafka topic aggregated_clicks и выбирает агрегаты
    только из последнего окна.

    Ожидаемый JSON:
    {
      "product_id": 42,
      "window_start_ms": 1710000000000,
      "window_end_ms": 1710000300000,
      "click_count": 123
    }
    """
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    topic_partition = TopicPartition(KAFKA_TOPIC_AGG, 0)
    consumer.assign([topic_partition])
    consumer.seek_to_beginning(topic_partition)

    messages: List[Dict[str, Any]] = []
    for msg in consumer:
        if isinstance(msg.value, dict):
            messages.append(msg.value)
    consumer.close()

    if not messages:
        context["ti"].xcom_push(
            key="aggregates",
            value={"window_start_ms": None, "window_end_ms": None, "by_product": {}},
        )
        return

    latest_end = max(int(m.get("window_end_ms", 0) or 0) for m in messages)
    latest_batch = [m for m in messages if int(m.get("window_end_ms", 0) or 0) == latest_end]

    by_product = {
        int(m["product_id"]): int(m["click_count"])
        for m in latest_batch
        if "product_id" in m and "click_count" in m
    }

    payload = {
        "window_start_ms": latest_batch[0].get("window_start_ms"),
        "window_end_ms": latest_end,
        "by_product": by_product,
    }
    context["ti"].xcom_push(key="aggregates", value=payload)


def _prepare_duckdb_orders(con: duckdb.DuckDBPyConnection) -> None:
    """Создаёт схемы и обновляет raw.orders из CSV-файла."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE SCHEMA IF NOT EXISTS dwh;")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.orders AS
        SELECT *
        FROM read_csv_auto('{ORDERS_CSV_PATH}', HEADER=TRUE);
    """)


def join_with_duckdb_orders(**context) -> None:
    """
    Читает агрегаты из Kafka, джойнится с историческими заказами через DuckDB
    и считает итоговый score.
    """
    agg = context["ti"].xcom_pull(key="aggregates", task_ids="read_kafka") or {}
    by_product: Dict[int, int] = agg.get("by_product", {}) or {}

    if not by_product:
        context["ti"].xcom_push(key="top10", value=[])
        return

    con = duckdb.connect(DUCKDB_PATH)
    try:
        _prepare_duckdb_orders(con)
        orders_rows = con.execute(
            """
            SELECT
                product_id,
                SUM(quantity) AS total_orders,
                SUM(quantity * price) AS revenue
            FROM raw.orders
            GROUP BY product_id
            """
        ).fetchall()
    finally:
        con.close()

    orders = {
        int(row[0]): {
            "total_orders": int(row[1] or 0),
            "revenue": float(row[2] or 0.0),
        }
        for row in orders_rows
    }

    window_start_ms = agg.get("window_start_ms")
    window_end_ms = agg.get("window_end_ms")
    window_start = (
        datetime.fromtimestamp((window_start_ms or 0) / 1000, tz=timezone.utc)
        if window_start_ms
        else None
    )
    window_end = (
        datetime.fromtimestamp((window_end_ms or 0) / 1000, tz=timezone.utc)
        if window_end_ms
        else None
    )

    final: List[Dict[str, Any]] = []
    for pid, clicks in by_product.items():
        order_info = orders.get(int(pid), {"total_orders": 0, "revenue": 0.0})
        score = WEIGHT_CLICKS * int(clicks) + WEIGHT_ORDERS * int(order_info["total_orders"])
        final.append(
            {
                "product_id": int(pid),
                "click_count": int(clicks),
                "total_orders": int(order_info["total_orders"]),
                "revenue": float(order_info["revenue"]),
                "score": float(score),
                "window_start": window_start.isoformat() if window_start else None,
                "window_end": window_end.isoformat() if window_end else None,
            }
        )

    final.sort(key=lambda item: item["score"], reverse=True)
    context["ti"].xcom_push(key="top10", value=final[:10])


def write_to_duckdb(**context) -> None:
    """Сохраняет итоговую витрину top_products в DuckDB."""
    top10 = context["ti"].xcom_pull(key="top10", task_ids="join_orders") or []

    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS dwh;")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS dwh.top_products (
                product_id INTEGER,
                click_count BIGINT,
                total_orders BIGINT,
                revenue DOUBLE,
                score DOUBLE,
                window_start VARCHAR,
                window_end VARCHAR,
                updated_at TIMESTAMP
            )
            """
        )

        con.execute("DELETE FROM dwh.top_products")

        for item in top10:
            con.execute(
                """
                INSERT INTO dwh.top_products
                (product_id, click_count, total_orders, revenue, score, window_start, window_end, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    item["product_id"],
                    item["click_count"],
                    item["total_orders"],
                    item["revenue"],
                    item["score"],
                    item["window_start"],
                    item["window_end"],
                ],
            )
    finally:
        con.close()


def write_to_redis(**context) -> None:
    """Сохраняет итоговую витрину в Redis через Airflow Connection redis_default."""
    top10 = context["ti"].xcom_pull(key="top10", task_ids="join_orders") or []

    redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
    redis_conn = redis_hook.get_conn()

    redis_conn.delete("top_products")
    redis_conn.delete("top_products_list")

    if not top10:
        return

    redis_conn.hset(
        "top_products",
        mapping={str(item["product_id"]): json.dumps(item, ensure_ascii=False) for item in top10},
    )
    redis_conn.set("top_products_list", json.dumps(top10, ensure_ascii=False), ex=600)


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["workshop", "recommendations", "duckdb"],
) as dag:
    t1 = PythonOperator(
        task_id="read_kafka",
        python_callable=read_latest_window_aggregates,
    )

    t2 = PythonOperator(
        task_id="join_orders",
        python_callable=join_with_duckdb_orders,
    )

    t3 = PythonOperator(
        task_id="write_duckdb",
        python_callable=write_to_duckdb,
    )

    t4 = PythonOperator(
        task_id="write_redis",
        python_callable=write_to_redis,
    )

    t1 >> t2 >> t3 >> t4