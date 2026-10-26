/*
    Измерение товаров.

    Grain:
        1 строка = 1 товар.

    Источник:
        fact_events.

    Атрибуты:
        - product_id;
        - category_id (FK -> dim_categories);
        - brand.

    Особенность:
        category_id и brand могут отсутствовать или меняться в событиях, берём последнее ненулевое значение по времени события.

    Цена не является атрибутом товара:
        price хранится на уровне события, поскольку может изменяться во времени.

    Материализация:
        table.
*/
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='product_id'
    )
}}

SELECT
    product_id,

    argMaxIf(
        category_id,
        event_time,
        category_id IS NOT NULL
    ) AS category_id,

    argMaxIf(
        brand,
        event_time,
        brand IS NOT NULL
    ) AS brand

FROM {{ source('core', 'fact_events') }}

GROUP BY
    product_id