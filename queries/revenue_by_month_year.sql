-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).

-- Ingresos por mes y año (2016-2018) con meses completos 01..12
-- revenue_by_month_year.sql
-- Ingresos por mes y año (2016–2018), 12 filas garantizadas
-- revenue_by_month_year.sql
-- Ingresos por mes y año (2016–2018) basados en pagos efectivos
WITH base AS (
  SELECT
    strftime('%Y', o.order_approved_at) AS y,
    strftime('%m', o.order_approved_at) AS m,
    SUM(COALESCE(oi.price, 0)) AS revenue
  FROM olist_orders_dataset AS o
  JOIN olist_order_items_dataset AS oi USING (order_id)
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND o.order_approved_at IS NOT NULL
  GROUP BY y, m
),
months(month_no, month) AS (
  SELECT '01', 'Jan' UNION ALL
  SELECT '02', 'Feb' UNION ALL
  SELECT '03', 'Mar' UNION ALL
  SELECT '04', 'Apr' UNION ALL
  SELECT '05', 'May' UNION ALL
  SELECT '06', 'Jun' UNION ALL
  SELECT '07', 'Jul' UNION ALL
  SELECT '08', 'Aug' UNION ALL
  SELECT '09', 'Sep' UNION ALL
  SELECT '10', 'Oct' UNION ALL
  SELECT '11', 'Nov' UNION ALL
  SELECT '12', 'Dec'
)
SELECT
  months.month_no,
  months.month,
  ROUND(COALESCE(MAX(CASE WHEN base.y = '2016' AND base.m = months.month_no THEN base.revenue END), 0), 2) AS Year2016,
  ROUND(COALESCE(MAX(CASE WHEN base.y = '2017' AND base.m = months.month_no THEN base.revenue END), 0), 2) AS Year2017,
  ROUND(COALESCE(MAX(CASE WHEN base.y = '2018' AND base.m = months.month_no THEN base.revenue END), 0), 2) AS Year2018
FROM months
LEFT JOIN base
  ON base.m = months.month_no
GROUP BY months.month_no, months.month
ORDER BY months.month_no;