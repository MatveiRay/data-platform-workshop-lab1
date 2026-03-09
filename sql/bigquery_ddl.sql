-- BigQuery DDL (пример)
-- Замените your-project на ваш project_id

CREATE TABLE IF NOT EXISTS `your-project.raw.clicks` (
  event_id STRING,
  user_id INT64,
  product_id INT64,
  category STRING,
  event_ts_ms INT64,
  event_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `your-project.raw.orders` (
  order_id INT64,
  user_id INT64,
  product_id INT64,
  quantity INT64,
  price FLOAT64,
  order_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `your-project.dwh.top_products` (
  product_id INT64,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  click_count INT64,
  total_orders INT64,
  revenue FLOAT64,
  score FLOAT64,
  updated_at TIMESTAMP
);
