/*
    Reconciliation: Core -> fact_sessions.

    Sessionization использует lookback 30 минут.
    Поэтому session_date не является корректным ключом
    для reconciliation событий batch.

    Проверка выполняется по временному интервалу сессии:

        Core event -> user_id + session_start <= event_time <= session_end -> ровно одна reconstructed session

    Проверяет:
        - каждое событие batch покрыто сессией;
        - ни одно событие не попадает более чем в одну сессию.

    Это учитывает seed event, который может находиться
    до начала batch и входить в агрегаты fact_sessions.

    Ожидаемый результат:
        0 строк.
*/

{% set batch_date = var('batch_date', none) %}

{% if batch_date %}

WITH batch_events AS (

    SELECT
        event_id,
        user_id,
        event_time
    FROM {{ source('core', 'fact_events') }}
    WHERE
        event_time >= toDateTime('{{ batch_date }} 00:00:00')
        AND event_time < addDays(
            toDateTime('{{ batch_date }} 00:00:00'),
            1
        )

),

matched_events AS (

    /*
    Используем LEFT JOIN, чтобы отловить события,
    которые не попали ни в одну сессию (session_matches = 0).
    Считаем только уникальные совпадения по session_id.
    */

    SELECT
        e.event_id,
        count(s.session_id) AS session_matches
    FROM batch_events AS e

    LEFT JOIN {{ ref('fact_sessions') }} AS s
        ON e.user_id = s.user_id
        AND e.event_time >= s.session_start
        AND e.event_time <= s.session_end

    GROUP BY e.event_id

),

reconciliation AS (

    SELECT
        count() AS core_events,
        countIf(session_matches = 1) AS matched_events,
        countIf(session_matches != 1) AS invalid_matches
    FROM matched_events

)

SELECT
    core_events,
    matched_events,
    invalid_matches
FROM reconciliation
WHERE
       core_events != matched_events
    OR invalid_matches != 0

{% else %}

SELECT
    1 AS test_failure
WHERE 1 = 0

{% endif %}