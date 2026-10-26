/*
    Дневная продуктовая витрина.

    Grain:
        дата * product

    Источник:
        fact_events

    Атрибуты:
        - event_date
        - product_id
        - category_id
        - category_root_name
        - category_leaf_name
        - brand
        - view_count
        - cart_count
        - remove_from_cart_count
        - purchase_count
        - revenue

    Назначение:
        - агрегированная аналитика по продуктам для BI, - продуктовые метрики: просмотры, добавления в корзину, удаления из корзины, покупки и выручка.

    Логика:
        - значения категории и бренда берутся из справочника товаров (dim_product),

    Материализация:
        incremental MergeTree.

    Incremental:
        при переданном batch_date пересчитывается только соответствующий день.
*/

{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(event_date, product_id)',
        partition_by='toYYYYMM(event_date)',
        pre_hook=[
            "{% if is_incremental() and var('batch_date', none) %}SET mutations_sync = 1{% endif %}",
            "{% if is_incremental() and var('batch_date', none) %}ALTER TABLE {{ this }} DELETE WHERE event_date = toDate('{{ var(\"batch_date\") }}'){% endif %}"
        ]
    )
}}

WITH product_events AS (

    SELECT
        event_date,
        product_id,
        countIf(event_type = 'view') AS view_count,
        countIf(event_type = 'cart') AS cart_count,
        countIf(event_type = 'remove_from_cart') AS remove_from_cart_count,
        countIf(event_type = 'purchase') AS purchase_count,
        sumIf(price, event_type = 'purchase') AS revenue
    FROM {{ source('core', 'fact_events') }}
    {{ incremental_filter('event_date') }}
    GROUP BY
        event_date,
        product_id
)

SELECT
    pe.event_date AS event_date,
    pe.product_id AS product_id,
    p.category_id AS category_id,
    c.category_root_name AS category_root_name,
    c.category_leaf_name AS category_leaf_name,
    p.brand AS brand,
    pe.view_count AS view_count,
    pe.cart_count AS cart_count,
    pe.remove_from_cart_count AS remove_from_cart_count,
    pe.purchase_count AS purchase_count,
    pe.revenue AS revenue
FROM product_events pe
LEFT JOIN {{ ref('dim_product') }} p ON pe.product_id = p.product_id
LEFT JOIN {{ ref('dim_category') }} c ON p.category_id = c.category_id