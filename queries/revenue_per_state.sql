-- revenue_per_state.sql
-- customer_state + Revenue (top 10)
SELECT
  c.customer_state,
  ROUND(SUM(oi.price + oi.freight_value), 2) AS Revenue
FROM olist_orders_dataset         AS o
JOIN olist_order_items_dataset    AS oi USING (order_id)
JOIN olist_customers_dataset      AS c  ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY Revenue DESC
LIMIT 10;