/*
    Витрина пользовательских сессий.

    Grain:
        1 строка = 1 реконструированная session.

    Источник:
        fact_sessions.

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

    Назначение:
        - aналитическая модель сессий для BI.

    Логика:
        Витрина сохраняет метрики, рассчитанные при sessionization:
        - количество событий;
        - просмотры;
        - добавления в корзину;
        - удаления из корзины;
        - покупки;
        - уникальные товары;
        - сумма покупок.

    Материализация:
        incremental MergeTree.
          
    Инкрементальность:
        Для корректного обновления сессий, пересекающих границу батча,
        удаляются и перезаписываются все записи, у которых session_end 
        попадает в окно обработки (batch_date минус lookback 30 минут).
*/

{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(session_start, user_id, session_id)',
        partition_by='toYYYYMM(session_start)',
        pre_hook=[
            "{% if is_incremental() and var('batch_date', none) %}SET mutations_sync = 1{% endif %}",
            "{% if is_incremental() and var('batch_date', none) %}ALTER TABLE {{ this }} DELETE WHERE session_end >= subtractMinutes(toDateTime('{{ var(\"batch_date\") }} 00:00:00'), 30){% endif %}"
        ]
    )
}}

SELECT
    session_id,
    user_id,
    session_start,
    session_end,
    session_duration_seconds,
    events_count,
    views_count,
    carts_count,
    remove_from_cart_count,
    purchases_count,
    products_count,
    purchase_amount,
    session_date

FROM {{ ref('fact_sessions') }}
{{ incremental_filter('session_date', 'session_end', 30) }}