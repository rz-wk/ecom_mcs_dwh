/*
 Проверка NOT NULL

 Ожидаемый результат: 0 строк
*/
SELECT
    category_id
FROM {{ ref('dim_category') }}
WHERE category_id IS NULL