{{ config(materialized='table') }}

SELECT
  product_id,
  COUNT(*) AS click_count,
  MAX(TIMESTAMP_MILLIS(event_ts_ms)) AS last_click_ts
FROM {{ source('raw', 'clicks') }}
WHERE TIMESTAMP_MILLIS(event_ts_ms) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
GROUP BY product_id
