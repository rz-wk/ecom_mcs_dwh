/*
 Проверка NOT NULL

 Ожидаемый результат: 0 строк
*/
SELECT
    product_id
FROM {{ ref('dim_product') }}
WHERE product_id IS NULL