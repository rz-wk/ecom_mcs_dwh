/*
    Reconciliation Core.

    Проверка, что в Core отсутствуют строки без
    корректных batch метаданных.
*/

SELECT
    event_id
FROM {{ source('core', 'fact_events') }}
WHERE
    _source_file IS NULL
    OR _batch_date IS NULL
    OR event_date IS NULL