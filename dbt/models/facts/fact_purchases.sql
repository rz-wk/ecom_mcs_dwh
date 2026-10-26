/*
    Факт событий покупки.

    Grain:
        1 строка = 1 purchase event

    Источник:
        fact_events

    Атрибуты:
        - event_id,
        - event_time,
        - event_type,
        - product_id,
        - category_id,
        - brand,
        - price,
        - user_id,
        - user_session,
        - event_date,
        - _source_file,
        - _batch_date,
        - _ingested_at.

    Назначение:
        выделение purchase-событий из атомарного факта событий
        для дальнейшей аналитики продаж.

    Модель не агрегирует покупки:
        каждая строка сохраняет исходный event_id,
        товар, пользователя, категорию, цену и время события.

    Материализация:
        incremental table.

    Incremental processing:
        обработка выполняется по event_date / batch_date.
*/
{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by=['event_date', 'event_id'],
        partition_by=['event_date'],
        unique_key='event_id',
        incremental_strategy='delete+insert'
    )
}}

WITH source_events AS (

    SELECT
        event_id,
        event_time,
        event_type,
        product_id,
        category_id,
        brand,
        price,
        user_id,
        user_session,
        event_date,
        _source_file,
        _batch_date,
        _ingested_at

    FROM {{ source('core', 'fact_events') }}

    {{ incremental_filter('event_date') }}
)

SELECT *
FROM source_events
WHERE event_type = 'purchase'