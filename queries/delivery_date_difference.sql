-- TODO: Esta consulta devolverá una tabla con dos columnas: Estado y 
-- Diferencia_Entrega. La primera contendrá las letras que identifican los 
-- estados, y la segunda mostrará la diferencia promedio entre la fecha estimada 
-- de entrega y la fecha en la que los productos fueron realmente entregados al 
-- cliente.
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. Puedes usar la función CAST para convertir un número a un entero.
-- 3. Puedes usar la función STRFTIME para convertir order_delivered_customer_date a una cadena, eliminando horas, minutos y segundos.
-- 4. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL


-- Diferencia promedio (en días) entre fecha real y estimada de entrega por estado (cliente)
-- delivery_date_difference.sql
-- Diferencia promedio entre fecha de entrega real y estimada, por estado

SELECT
  c.customer_state AS State,
  CAST(
    AVG(
      JULIANDAY(DATE(o.order_estimated_delivery_date))
      - JULIANDAY(DATE(o.order_delivered_customer_date))
    ) AS INT
  ) AS Delivery_Difference
FROM olist_orders_dataset AS o
JOIN olist_customers_dataset AS c
  ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY Delivery_Difference ASC, State;