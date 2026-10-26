/*
    Проверка валидности витрины сессий.

    Проверяет:
        - session_end >= session_start;
        - длительность неотрицательна;
        - events_count > 0;
        - все агрегаты неотрицательны.

    Ожидаемый результат: 0 строк.
*/
SELECT
    session_id
FROM {{ ref('mart_sessions') }}
WHERE
       session_end < session_start
    OR session_duration_seconds < 0
    OR events_count <= 0
    OR views_count < 0
    OR carts_count < 0
    OR remove_from_cart_count < 0
    OR purchases_count < 0
    OR products_count < 0
    OR purchase_amount < 0