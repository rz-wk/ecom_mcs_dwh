/*
 Проверка NOT NULL

 Ожидаемый результат: 0 строк
*/
SELECT
    event_id
FROM {{ ref('fact_purchases') }}
WHERE
    event_id IS NULL
    OR event_time IS NULL
    OR product_id IS NULL
    OR user_id IS NULL
    OR price IS NULL
    OR event_date IS NULL