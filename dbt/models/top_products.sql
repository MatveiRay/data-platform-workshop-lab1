{{ config(materialized='table') }}

WITH popular AS (
  SELECT * FROM {{ ref('popular_products') }}
),
orders_agg AS (
  SELECT
    product_id,
    SUM(quantity) AS total_orders,
    SUM(quantity * price) AS revenue
  FROM {{ source('raw', 'orders') }}
  GROUP BY product_id
)
SELECT
  p.product_id,
  p.click_count,
  COALESCE(o.total_orders, 0) AS total_orders,
  COALESCE(o.revenue, 0) AS revenue,
  (p.click_count * 0.7 + COALESCE(o.total_orders, 0) * 0.3) AS score,
  CURRENT_TIMESTAMP() AS updated_at
FROM popular p
LEFT JOIN orders_agg o USING(product_id)
ORDER BY score DESC
LIMIT 10
