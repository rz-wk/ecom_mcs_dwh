/*
    Reconciliation Core -> Marts.

    Назначение:
        Проверяет, что ключевые агрегаты четырех аналитических
        витрин соответствуют исходным данным Core.

    Проверяемые витрины:
        - mart_funnel;
        - mart_products;
        - mart_users;
        - mart_sales.

    Не проверяется:
        - mart_sessions — sessionization является независимой
          реконструкцией и не имеет построчного соответствия
          с атомарными событиями Core.

    Правило:
        При расхождении хотя бы одной агрегированной метрики
        тест возвращает строки и завершается ошибкой.

    Сравниваем:
        - количество view;
        - количество cart;
        - количество remove_from_cart;
        - количество purchase;
        - purchase/revenue.

    Все метрики рассчитываются за один batch_date.
*/

{% set batch_date = var('batch_date', none) %}

{% if batch_date %}

WITH

core_metrics AS (

    SELECT
        countIf(event_type = 'view') AS views_count,
        countIf(event_type = 'cart') AS carts_count,
        countIf(event_type = 'remove_from_cart') AS removes_count,
        countIf(event_type = 'purchase') AS purchases_count,
        sumIf(price, event_type = 'purchase') AS purchase_amount
    FROM {{ source('core', 'fact_events') }}
    WHERE event_date = toDate('{{ batch_date }}')
),

funnel_metrics AS (

    SELECT
        sumIf(stage_count, funnel_stage = 'view') AS views_count,
        sumIf(stage_count, funnel_stage = 'cart') AS carts_count,
        sumIf(stage_count, funnel_stage = 'remove_from_cart') AS removes_count,
        sumIf(stage_count, funnel_stage = 'purchase') AS purchases_count,
        sum(revenue) AS purchase_amount
    FROM {{ ref('mart_funnel') }}
    WHERE event_date = toDate('{{ batch_date }}')
),

product_metrics AS (

    SELECT
        sum(view_count) AS views_count,
        sum(cart_count) AS carts_count,
        sum(remove_from_cart_count) AS removes_count,
        sum(purchase_count) AS purchases_count,
        sum(revenue) AS purchase_amount
    FROM {{ ref('mart_products') }}
    WHERE event_date = toDate('{{ batch_date }}')
),

user_metrics AS (

    SELECT
        sum(view_count) AS views_count,
        sum(cart_count) AS carts_count,
        sum(remove_from_cart_count) AS removes_count,
        sum(purchase_count) AS purchases_count,
        sum(revenue) AS purchase_amount
    FROM {{ ref('mart_users') }}
    WHERE event_date = toDate('{{ batch_date }}')
),

sales_metrics AS (

    SELECT
        sum(purchase_count) AS purchases_count,
        sum(revenue) AS purchase_amount
    FROM {{ ref('mart_sales') }}
    WHERE purchase_date = toDate('{{ batch_date }}')
)

SELECT
    'mart_funnel' AS mart,
    core.views_count AS core_views,
    funnel.views_count AS mart_views,
    core.carts_count AS core_carts,
    funnel.carts_count AS mart_carts,
    core.removes_count AS core_removes,
    funnel.removes_count AS mart_removes,
    core.purchases_count AS core_purchases,
    funnel.purchases_count AS mart_purchases,
    core.purchase_amount AS core_revenue,
    funnel.purchase_amount AS mart_revenue
FROM core_metrics AS core
CROSS JOIN funnel_metrics AS funnel
WHERE
    core.views_count != funnel.views_count
    OR core.carts_count != funnel.carts_count
    OR core.removes_count != funnel.removes_count
    OR core.purchases_count != funnel.purchases_count
    OR core.purchase_amount != funnel.purchase_amount

UNION ALL

SELECT
    'mart_products' AS mart,
    core.views_count,
    product.views_count,
    core.carts_count,
    product.carts_count,
    core.removes_count,
    product.removes_count,
    core.purchases_count,
    product.purchases_count,
    core.purchase_amount,
    product.purchase_amount
FROM core_metrics AS core
CROSS JOIN product_metrics AS product
WHERE
    core.views_count != product.views_count
    OR core.carts_count != product.carts_count
    OR core.removes_count != product.removes_count
    OR core.purchases_count != product.purchases_count
    OR core.purchase_amount != product.purchase_amount

UNION ALL

SELECT
    'mart_users' AS mart,
    core.views_count,
    users.views_count,
    core.carts_count,
    users.carts_count,
    core.removes_count,
    users.removes_count,
    core.purchases_count,
    users.purchases_count,
    core.purchase_amount,
    users.purchase_amount
FROM core_metrics AS core
CROSS JOIN user_metrics AS users
WHERE
    core.views_count != users.views_count
    OR core.carts_count != users.carts_count
    OR core.removes_count != users.removes_count
    OR core.purchases_count != users.purchases_count
    OR core.purchase_amount != users.purchase_amount

UNION ALL

SELECT
    'mart_sales' AS mart,
    core.views_count,
    0 AS mart_views,
    core.carts_count,
    0 AS mart_carts,
    core.removes_count,
    0 AS mart_removes,
    core.purchases_count,
    sales.purchases_count,
    core.purchase_amount,
    sales.purchase_amount
FROM core_metrics AS core
CROSS JOIN sales_metrics AS sales
WHERE
    core.purchases_count != sales.purchases_count
    OR core.purchase_amount != sales.purchase_amount

{% else %}

/*
    Без batch_date проверка не выполняется:
    reconciliation предназначен для конкретного
    пересчитанного incremental-периода.
*/

SELECT
    'batch_date is required' AS error

WHERE 1 = 0

{% endif %}