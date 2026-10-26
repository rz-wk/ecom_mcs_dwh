/*
 Проверка уникальности

 Ожидаемый результат: 0 строк
*/
SELECT
    event_id,
    count(*) AS occurrences
FROM {{ ref('fact_purchases') }}
GROUP BY event_id
HAVING count(*) > 1