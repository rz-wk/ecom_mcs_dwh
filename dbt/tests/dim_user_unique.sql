/*
 Проверка уникальности пользователей

 Ожидаемый результат: 0 строк
*/
SELECT
    user_id,
    count(*) AS occurrences

FROM {{ ref('dim_user') }}
GROUP BY user_id
HAVING count(*) > 1