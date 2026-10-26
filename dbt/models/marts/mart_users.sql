/*
    Дневная пользовательская витрина.

    Grain:
        дата * user

    Источник:
        fact_events

    Атрибуты:
        - event_date
        - user_id
        - event_count
        - view_count
        - cart_count
        - remove_from_cart_count
        - purchase_count
        - revenue
        - source_session_count

    Назначение:
        поведенческие метрики пользователя за день.
*/

{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(event_date, user_id)',
        partition_by='toYYYYMM(event_date)',
        pre_hook=[
            "{% if is_incremental() and var('batch_date', none) %}SET mutations_sync = 1{% endif %}",
            "{% if is_incremental() and var('batch_date', none) %}ALTER TABLE {{ this }} DELETE WHERE event_date = toDate('{{ var(\"batch_date\") }}'){% endif %}"
        ]
    )
}}

SELECT
    event_date,
    user_id,
    count() AS event_count,
    countIf(event_type = 'view') AS view_count,
    countIf(event_type = 'cart') AS cart_count,
    countIf(event_type = 'remove_from_cart') AS remove_from_cart_count,
    countIf(event_type = 'purchase') AS purchase_count,
    sumIf(price, event_type = 'purchase') AS revenue,
    uniqExact(user_session) AS source_session_count
FROM {{ source('core', 'fact_events') }}
{{ incremental_filter('event_date') }}
GROUP BY
    event_date,
    user_id