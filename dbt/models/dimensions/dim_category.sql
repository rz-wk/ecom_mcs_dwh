/*
    Измерение категорий товаров.

    Grain:
        1 строка = 1 категория

    Источник:
        fact_events

    Атрибуты:
        - category_id;
        - category_code;
        - category_path;
        - category_root_name;
        - category_leaf_name.

    Атрибуты формируются из category_id и category_code.
    category_code разбивается на массив уровней (category_path),
    а также выделяются корневой (category_root_name) и конечный (category_leaf_name) уровни.
    Вычисление производных полей вынесено в CTE для безопасной обработки NULL.

    Независимого master-data источника категорий
    в проекте нет, поэтому измерение является производным
    от атомарного fact_events.

    Материализация:
        table.
*/
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='category_id'
    )
}}

WITH latest_category_codes AS (
    SELECT
        category_id,
        argMaxIf(
            category_code,
            event_time,
            category_code IS NOT NULL
        ) AS category_code
    FROM {{ source('core', 'fact_events') }}
    WHERE category_id IS NOT NULL
    GROUP BY category_id
)

SELECT
    category_id,
    category_code,

    -- Массив всех уровней категории
    splitByChar('.', category_code) AS category_path,

    -- Корневой уровень
    splitByChar('.', category_code)[1] AS category_root_name,

    -- Конечный уровень
    arrayElement(splitByChar('.', category_code), -1) AS category_leaf_name

FROM latest_category_codes