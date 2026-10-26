/*
    Макрос для фильтрации инкрементальных моделей.
    Поддерживает стандартную фильтрацию по дате и фильтрацию со сдвигом (lookback).
*/
{% macro incremental_filter(date_col, lookback_col=none, lookback_minutes=0) %}

    {% if is_incremental() and var('batch_date', none) %}

        {% if lookback_col %}
            WHERE {{ lookback_col }} >= subtractMinutes(toDateTime('{{ var("batch_date") }} 00:00:00'), {{ lookback_minutes }})
        {% else %}
            WHERE {{ date_col }} = toDate('{{ var("batch_date") }}')
        {% endif %}

    {% elif is_incremental() %}

        WHERE {{ date_col }} > (
            SELECT max({{ date_col }})
            FROM {{ this }}
        )

    {% elif var('batch_date', none) %}

        WHERE {{ date_col }} = toDate('{{ var("batch_date") }}')

    {% endif %}

{% endmacro %}