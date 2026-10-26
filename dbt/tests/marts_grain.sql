/*
    Контроль физического grain аналитических витрин.

    Каждая модель должна содержать не более одной строки
    на заявленный grain.

    Для `mart_sessions` grain контролируется `unique(session_id)`
    в `models/marts/schema.yml`.
*/

SELECT
    'mart_products' AS model_name,
    event_date,
    toString(product_id) AS entity_id,
    count() AS duplicate_rows
FROM {{ ref('mart_products') }}
GROUP BY
    event_date,
    product_id
HAVING count() > 1

UNION ALL

SELECT
    'mart_users' AS model_name,
    event_date,
    toString(user_id) AS entity_id,
    count() AS duplicate_rows
FROM {{ ref('mart_users') }}
GROUP BY
    event_date,
    user_id
HAVING count() > 1

UNION ALL

SELECT
    'mart_sales' AS model_name,
    purchase_date AS event_date,
    concat(
        toString(category_id),
        '|',
        coalesce(brand, '')
    ) AS entity_id,
    count() AS duplicate_rows
FROM {{ ref('mart_sales') }}
GROUP BY
    purchase_date,
    category_id,
    brand
HAVING count() > 1

UNION ALL

SELECT
    'mart_funnel' AS model_name,
    event_date,
    concat(
        toString(category_id),
        '|',
        coalesce(brand, ''),
        '|',
        funnel_stage
    ) AS entity_id,
    count() AS duplicate_rows
FROM {{ ref('mart_funnel') }}
GROUP BY
    event_date,
    category_id,
    brand,
    funnel_stage
HAVING count() > 1