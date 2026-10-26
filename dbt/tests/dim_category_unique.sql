/*
 Проверка уникальности

 Ожидаемый результат: 0 строк
*/
SELECT
    category_id,
    count(*) AS occurrences
FROM {{ ref('dim_category') }}
GROUP BY category_id
HAVING count(*) > 1