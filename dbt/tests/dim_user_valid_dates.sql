/*
 Проверка валидности дат

 Ожидаемый результат: 0 строк
*/
SELECT
    user_id,
    first_seen,
    last_seen
FROM {{ ref('dim_user') }}
WHERE
    user_id IS NULL
    OR first_seen IS NULL
    OR last_seen IS NULL
    OR first_seen > last_seen