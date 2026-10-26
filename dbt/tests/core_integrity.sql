/*
    Базовая целостность Core.

    Проверяет:
        - уникальность event_id;
        - допустимые event_type;
        - отсутствие отрицательной цены;
        - отсутствие NULL в обязательных полях.
*/

SELECT
    event_id
FROM {{ source('core', 'fact_events') }}
WHERE
    event_id IS NULL
    OR event_time IS NULL
    OR event_type IS NULL
    OR product_id IS NULL
    OR user_id IS NULL
    OR user_session IS NULL
    OR price < 0
    OR event_type NOT IN (
        'view',
        'cart',
        'remove_from_cart',
        'purchase'
    )