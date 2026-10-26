/*
    Контроль количественных метрик `mart_funnel`:
    отсутствия отрицательных значений
    количества событий и выручки.

    Прим. `remove_from_cart` не участвует в проверке
    purchase <= cart <= view, поскольку это не этап
    последовательной конверсии, а отдельное действие.

    Ожидаемый результат: 0 строк
*/

SELECT
    event_date,
    category_id,
    brand,
    funnel_stage,
    stage_count,
    revenue
FROM {{ ref('mart_funnel') }}
WHERE
    stage_count < 0
    OR revenue < 0