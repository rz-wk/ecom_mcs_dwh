/*
    Измерение пользователей.

    Grain:
        1 строка = 1 пользователь

    Источник:
        fact_events

    Атрибуты:
        - user_id;
        - first_seen;
        - last_seen.

    Модель содержит только базовые идентификационные
    и временные атрибуты пользователя.

    Поведенческие агрегаты намеренно не включаются:
        они относятся к аналитическому слою mart_users.

    Материализация:
        incremental table.
*/
{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='user_id',
        unique_key='user_id',
        incremental_strategy='delete+insert'
    )
}}

{% if is_incremental() %}

WITH affected_users AS (

    SELECT DISTINCT
        user_id

    FROM {{ source('core', 'fact_events') }}

    WHERE event_date = toDate('{{ var("batch_date") }}')
)

SELECT
    e.user_id,
    min(e.event_time) AS first_seen,
    max(e.event_time) AS last_seen

FROM {{ source('core', 'fact_events') }} AS e

INNER JOIN affected_users AS u
    ON e.user_id = u.user_id

GROUP BY
    e.user_id

{% else %}

SELECT
    user_id,
    min(event_time) AS first_seen,
    max(event_time) AS last_seen

FROM {{ source('core', 'fact_events') }}

GROUP BY
    user_id

{% endif %}