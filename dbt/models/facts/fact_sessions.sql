/*
    Факт пользовательских сессий.
    Sessionization событий из fact_events.

    Grain:
        1 строка = 1 реконструированная пользовательская сессия

    Источник:
        fact_events

    Атрибуты:
        - session_id,
        - user_id,
        - session_start,
        - session_end,
        - session_duration_seconds,
        - events_count,
        - views_count,
        - carts_count,
        - remove_from_cart_count,
        - purchases_count,
        - products_count,
        - purchase_amount,
        - session_date.
        
    session_id:
        детерминированный идентификатор:
            hash(user_id, session_start)

    Sessionization:
        - события группируются по user_id;
        - предыдущий event_time определяется через LAG;
        - разрыв более 30 минут начинает новую сессию;
        - session_id формируется как накопительный номер сессии.

    Логика:
        - новое событие начинает новую сессию, если интервал
        от предыдущего события пользователя строго больше 30 минут.

    Особенность:
        sessionization является независимой реконструкцией
        и не использует исходный user_session.
        Сессия реконструируется независимо по event_time.

    Материализация:
        incremental table.

    Incremental processing:
        Для инкремента определяются пользователи, у которых есть события в текущем батче.
        Все их старые сессии удаляются (синхронно).
        Затем все их сессии пересчитываются с нуля по всей истории событий.
        Это гарантирует отсутствие дубликатов и потерянных данных при пересечении границы батча.
*/
{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(user_id, session_start, session_id)',
        partition_by='toYYYYMM(session_start)',
        pre_hook=[
            "{% if is_incremental() and var('batch_date', none) %}ALTER TABLE {{ this }} DELETE WHERE user_id IN (SELECT user_id FROM {{ source('core', 'fact_events') }} WHERE event_date = toDate('{{ var(\"batch_date\") }}')) SETTINGS mutations_sync = 1{% endif %}"
        ]
    )
}}

{% set session_timeout_minutes = 30 %}

{% if is_incremental() %}

    {% set target_start %}
        toDateTime('{{ var("batch_date") }} 00:00:00')
    {% endset %}

{% endif %}

WITH source_events AS (

    SELECT
        event_id, event_time, event_type, product_id, category_id,
        category_code, brand, price, user_id, event_date
    FROM {{ source('core', 'fact_events') }}

    {% if is_incremental() %}

    /*
    Берем все события для пользователей, у которых есть события в текущем батче.
    */
    WHERE user_id IN (
        SELECT DISTINCT user_id
        FROM {{ source('core', 'fact_events') }}
        WHERE event_date = toDate('{{ var("batch_date") }}')
    )

    {% else %}

    WHERE 1 = 1

    {% endif %}
),

ordered_events AS (
    /*
    Определяем предыдущее событие пользователя.
    */
    SELECT
        *,
        lagInFrame(event_time, 1)
            OVER (
                PARTITION BY user_id
                ORDER BY event_time, event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS previous_event_time
    FROM source_events
),

marked_events AS (
    /*
    Определяем флаг начала новой сессии.
    */
    SELECT
        *,
        if(
            previous_event_time IS NULL
            OR dateDiff(
                'second',
                previous_event_time,
                event_time
            ) > {{ session_timeout_minutes * 60 }},
            1,
            0
        ) AS is_new_session
    FROM ordered_events
),

sessionized_events AS (
    /*
    Вычисляем порядковый номер сессии пользователя.
    */
    SELECT
        *,
        sum(is_new_session)
            OVER (
                PARTITION BY user_id
                ORDER BY event_time, event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS session_number
    FROM marked_events
),

with_session_start AS (
    /*
    Определяем session_start как min(event_time) в группе.
    */
    SELECT
        *,
        min(event_time)
            OVER (
                PARTITION BY user_id, session_number
            ) AS session_start
    FROM sessionized_events
),

with_session_id AS (
    /*
    Формируем session_id.
    */
    SELECT
        *,
        hex(
            cityHash64(
                toString(user_id),
                toString(session_start)
            )
        ) AS session_id
    FROM with_session_start
),

sessions AS (
    /*
    Агрегируем данные до уровня сессии.
    */
    SELECT
        session_id,
        user_id,
        min(session_start) AS session_start,
        max(event_time) AS session_end,

        count() AS events_count,

        countIf(event_type = 'view') AS views_count,
        countIf(event_type = 'cart') AS carts_count,
        countIf(event_type = 'remove_from_cart')
            AS remove_from_cart_count,
        countIf(event_type = 'purchase') AS purchases_count,

        uniqExact(product_id) AS products_count,

        sumIf(
            price,
            event_type = 'purchase'
        ) AS purchase_amount
    FROM with_session_id
    GROUP BY
        session_id,
        user_id
)

SELECT
    session_id,
    user_id,
    session_start,
    session_end,

    dateDiff(
        'second',
        session_start,
        session_end
    ) AS session_duration_seconds,

    events_count,
    views_count,
    carts_count,
    remove_from_cart_count,
    purchases_count,
    products_count,
    purchase_amount,

    toDate(session_start) AS session_date

FROM sessions