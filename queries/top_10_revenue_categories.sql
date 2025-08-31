-- TODO: Esta consulta devolverá una tabla con las 10 categorías con mayores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con mayores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.

WITH base AS (
  SELECT
    t.product_category_name_english AS Category,
    o.order_id,
    (oi.price + oi.freight_value) AS revenue
  FROM olist_orders_dataset AS o
  JOIN olist_order_items_dataset AS oi USING (order_id)
  JOIN olist_products_dataset AS p USING (product_id)
  LEFT JOIN product_category_name_translation AS t
    ON t.product_category_name = p.product_category_name
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND t.product_category_name_english IS NOT NULL
)
SELECT
  Category,
  COUNT(DISTINCT order_id) AS Num_order,
  ROUND(SUM(revenue), 2)    AS Revenue
FROM base
GROUP BY Category
ORDER BY Revenue DESC, Category
LIMIT 10;
