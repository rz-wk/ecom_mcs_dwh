/*
    Витрина воронки пользовательских событий.

    Grain:
        дата * категория * бренд * funnel stage

    Источник:
        fact_events

    Атрибуты:
        - event_date
        - category_id
        - category_root_name
        - category_leaf_name
        - brand
        - funnel_stage
        - stage_count
        - revenue

    Назначение:
        - агрегированная аналитика событий для BI.

    Логика:
        - агрегирует атомарные события по дате, категории и бренду;
        - каждое событие становится отдельной строкой stage;
        - количество событий сохраняется как stage_count;
        - purchase дополнительно содержит revenue.
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
        order_by='(event_date, category_id, brand, funnel_stage)',
        partition_by='toYYYYMM(event_date)',
        pre_hook=[
            "{% if is_incremental() and var('batch_date', none) %}SET mutations_sync = 1{% endif %}",
            "{% if is_incremental() and var('batch_date', none) %}ALTER TABLE {{ this }} DELETE WHERE event_date = toDate('{{ var(\"batch_date\") }}'){% endif %}"
        ]
    )
}}

WITH source_events AS (

    SELECT
        event_date,
        ifNull(category_id, 0) AS category_id,
        ifNull(brand, '') AS brand,
        event_type,
        price
    FROM {{ source('core', 'fact_events') }}
    {{ incremental_filter('event_date') }}
),

funnel AS (

    SELECT
        event_date,
        category_id,
        brand,
        event_type AS funnel_stage,
        count() AS stage_count,
        sumIf(price, event_type = 'purchase') AS revenue
    FROM source_events
    WHERE event_type IN (
        'view',
        'cart',
        'remove_from_cart',
        'purchase'
    )
    GROUP BY
        event_date,
        category_id,
        brand,
        funnel_stage
)

SELECT
    f.event_date,
    f.category_id,
    c.category_root_name,
    c.category_leaf_name,
    f.brand,
    f.funnel_stage,
    f.stage_count,
    f.revenue
FROM funnel f
LEFT JOIN {{ ref('dim_category') }} c ON f.category_id = c.category_id