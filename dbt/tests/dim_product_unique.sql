/*
  Проверка уникальности

 Ожидаемый результат: 0 строк
*/
SELECT
    product_id,
    count(*) AS occurrences

FROM {{ ref('dim_product') }}
GROUP BY product_id
HAVING count(*) > 1