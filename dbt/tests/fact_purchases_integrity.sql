/*
 Проверка, что каждый purchase действительно существует в Core и имеет тот же набор ключевых атрибутов

 Ожидаемый результат: 0 строк
*/
SELECT
    p.event_id
FROM {{ ref('fact_purchases') }} AS p
LEFT JOIN {{ source('core', 'fact_events') }} AS e
    ON p.event_id = e.event_id
WHERE
    e.event_id IS NULL
    OR e.event_type != 'purchase'