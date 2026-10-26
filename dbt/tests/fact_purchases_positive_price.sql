/*
 Проверка отрицательных цен

 Ожидаемый результат: 0 строк
*/
SELECT
    event_id,
    price
FROM {{ ref('fact_purchases') }}
WHERE price < 0