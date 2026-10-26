/*
    Reconciliation: Core -> fact_purchases.

    Количество purchase-событий в производном fact_purchases должно
    совпадать с количеством purchase-событий в fact_events.

    Ожидаемый результат:
        0 строк.
*/

{% set batch_date = var('batch_date', none) %}

{% if batch_date %}

WITH core AS (
    SELECT count(*) AS core_count
    FROM {{ source('core', 'fact_events') }}
    WHERE
        _batch_date = toDate('{{ batch_date }}')
        AND event_type = 'purchase'
),

purchases AS (
    SELECT count(*) AS purchase_count
    FROM {{ ref('fact_purchases') }}
    WHERE event_date = toDate('{{ batch_date }}')
)

SELECT
    core.core_count,
    purchases.purchase_count
FROM core
CROSS JOIN purchases
WHERE core.core_count != purchases.purchase_count

{% else %}

SELECT 1 AS test_failure
WHERE 1 = 0

{% endif %}