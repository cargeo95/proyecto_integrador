-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.


-- Tiempos real vs estimado por mes y año (2016-2018)

WITH months(month_no, month) AS (
  SELECT '01','Jan' UNION ALL SELECT '02','Feb' UNION ALL SELECT '03','Mar' UNION ALL
  SELECT '04','Apr' UNION ALL SELECT '05','May' UNION ALL SELECT '06','Jun' UNION ALL
  SELECT '07','Jul' UNION ALL SELECT '08','Aug' UNION ALL SELECT '09','Sep' UNION ALL
  SELECT '10','Oct' UNION ALL SELECT '11','Nov' UNION ALL SELECT '12','Dec'
),
orders_clean AS (
  -- Órdenes únicas con tiempos (en días)
  SELECT DISTINCT
    o.order_id,
    strftime('%Y', o.order_purchase_timestamp) AS y,
    strftime('%m', o.order_purchase_timestamp) AS m,
    (julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)) AS real_days,
    (julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp)) AS est_days
  FROM olist_orders_dataset AS o
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
),
agg AS (
  SELECT
    y, m,
    AVG(real_days) AS avg_real,
    AVG(est_days)  AS avg_est
  FROM orders_clean
  GROUP BY y, m
)
SELECT
  months.month_no,
  months.month,
  ROUND((SELECT avg_real FROM agg WHERE y='2016' AND m=months.month_no), 2) AS Year2016_real_time,
  ROUND((SELECT avg_real FROM agg WHERE y='2017' AND m=months.month_no), 2) AS Year2017_real_time,
  ROUND((SELECT avg_real FROM agg WHERE y='2018' AND m=months.month_no), 2) AS Year2018_real_time,
  ROUND((SELECT avg_est  FROM agg WHERE y='2016' AND m=months.month_no), 2) AS Year2016_estimated_time,
  ROUND((SELECT avg_est  FROM agg WHERE y='2017' AND m=months.month_no), 2) AS Year2017_estimated_time,
  ROUND((SELECT avg_est  FROM agg WHERE y='2018' AND m=months.month_no), 2) AS Year2018_estimated_time
FROM months
ORDER BY months.month_no;
