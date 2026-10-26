/*
    Общий контроль числовых метрик аналитических витрин.

    Отрицательные значения для количества событий,
    количества пользователей, товаров и выручки
    не соответствуют бизнес-смыслу моделей.
*/

SELECT
    'mart_products' AS model_name,
    event_date,
    toString(product_id) AS entity_id,
    view_count,
    cart_count,
    remove_from_cart_count,
    purchase_count,
    revenue
FROM {{ ref('mart_products') }}
WHERE
    view_count < 0
    OR cart_count < 0
    OR remove_from_cart_count < 0
    OR purchase_count < 0
    OR revenue < 0

UNION ALL

SELECT
    'mart_users' AS model_name,
    event_date,
    toString(user_id) AS entity_id,
    view_count,
    cart_count,
    remove_from_cart_count,
    purchase_count,
    revenue
FROM {{ ref('mart_users') }}
WHERE
    event_count < 0
    OR view_count < 0
    OR cart_count < 0
    OR remove_from_cart_count < 0
    OR purchase_count < 0
    OR revenue < 0

UNION ALL

SELECT
    'mart_sales' AS model_name,
    purchase_date AS event_date,
    toString(category_id) AS entity_id,
    0 AS view_count,
    0 AS cart_count,
    0 AS remove_from_cart_count,
    purchase_count,
    revenue
FROM {{ ref('mart_sales') }}
WHERE
    purchase_count < 0
    OR purchasing_users < 0
    OR purchased_products < 0
    OR revenue < 0
    OR avg_purchase_price < 0