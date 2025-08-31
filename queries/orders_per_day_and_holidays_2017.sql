WITH RECURSIVE days(d) AS (
  SELECT date('2017-01-01')
  UNION ALL
  SELECT date(d, '+1 day') FROM days WHERE d < date('2017-12-31')
),
orders_2017 AS (
  SELECT date(o.order_purchase_timestamp) AS d, o.order_id
  FROM olist_orders_dataset AS o
  WHERE strftime('%Y', o.order_purchase_timestamp) = '2017'
),
holidays AS (
  SELECT date AS d FROM public_holidays WHERE strftime('%Y', date) = '2017'
)
SELECT
  days.d AS date,
  COUNT(DISTINCT orders_2017.order_id) AS num_orders,
  CASE WHEN holidays.d IS NOT NULL THEN 1 ELSE 0 END AS is_holiday
FROM days
LEFT JOIN orders_2017 ON orders_2017.d = days.d
LEFT JOIN holidays   ON holidays.d   = days.d
GROUP BY days.d
ORDER BY days.d;