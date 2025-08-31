SELECT
  p.product_id,
  p.product_weight_g,
  ROUND(AVG(oi.freight_value), 2) AS avg_freight_value
FROM olist_order_items_dataset AS oi
JOIN olist_products_dataset AS p USING (product_id)
JOIN olist_orders_dataset AS o USING (order_id)
WHERE o.order_status = 'delivered'
  AND p.product_weight_g IS NOT NULL
GROUP BY p.product_id, p.product_weight_g
ORDER BY p.product_weight_g, avg_freight_value;
