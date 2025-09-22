-- TODO: Esta consulta devolverá una tabla con dos columnas: estado_pedido y
-- Cantidad. La primera contendrá las diferentes clases de estado de los pedidos,
-- y la segunda mostrará el total de cada uno.

-- Cantidad de pedidos agrupados por estado
-- global_ammount_order_status.sql
SELECT
  o.order_status,
  COUNT(*) AS Ammount
FROM olist_orders_dataset AS o
GROUP BY o.order_status
ORDER BY o.order_status;
