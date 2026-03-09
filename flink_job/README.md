# Flink / PyFlink job

## Что делает `window_aggregation.py`
- Читает клики из Kafka topic `clicks`
- Считает `click_count` по каждому `product_id` в **tumbling window 5 минут**
- Публикует агрегаты в `aggregated_clicks` (JSON) вместе с границами окна

## Kafka connector JAR
PyFlink требует Kafka connector JAR в classpath Flink.

Скачайте JAR для **Flink 1.18** (примерное имя):
- `flink-sql-connector-kafka-3.0.1-1.18.jar`

и положите его в `flink_job/jars/`.

## Запуск
```bash
docker exec -it dpw-flink-jobmanager bash -lc "flink run -py /opt/flink/usrlib/window_aggregation.py"
```

UI Flink: http://localhost:8081
