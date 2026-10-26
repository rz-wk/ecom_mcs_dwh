/*
    Дневная витрина продаж.

    Grain:
        дата * категория * бренд

    Источник:
        fact_purchases

    Атрибуты:
        - purchase_date
        - category_id
        - category_root_name
        - category_leaf_name
        - brand
        - purchase_count
        - purchasing_users
        - purchased_products
        - revenue
        - avg_purchase_price

    Назначение:
        - агрегированная аналитика продаж для BI.

    Логика:
        - каждая строка содержит агрегаты purchase events
        за конкретную дату, категорию и бренд.
        - имена категорий подтягиваются из dim_category.
    
    Материализация:
        incremental MergeTree.

    Incremental:
        при переданном batch_date пересчитывается только соответствующий день.
*/

{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(purchase_date, category_id, brand)',
        partition_by='toYYYYMM(purchase_date)',
        pre_hook=[
            "{% if is_incremental() and var('batch_date', none) %}SET mutations_sync = 1{% endif %}",
            "{% if is_incremental() and var('batch_date', none) %}ALTER TABLE {{ this }} DELETE WHERE purchase_date = toDate('{{ var(\"batch_date\") }}'){% endif %}"
        ]
    )
}}

WITH purchases AS (

    SELECT
        event_date AS purchase_date,
        ifNull(category_id, 0) AS category_id,
        ifNull(brand, '') AS brand,
        user_id,
        product_id,
        price
    FROM {{ ref('fact_purchases') }}
    {{ incremental_filter('event_date') }}
)

SELECT
    p.purchase_date,
    p.category_id,
    c.category_root_name,
    c.category_leaf_name,
    p.brand,
    count() AS purchase_count,
    uniqExact(p.user_id) AS purchasing_users,
    uniqExact(p.product_id) AS purchased_products,
    sum(p.price) AS revenue,
    avg(p.price) AS avg_purchase_price
FROM purchases p
LEFT JOIN {{ ref('dim_category') }} c ON p.category_id = c.category_id
GROUP BY
    p.purchase_date,
    p.category_id,
    c.category_root_name,
    c.category_leaf_name,
    p.brand