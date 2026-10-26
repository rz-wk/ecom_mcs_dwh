# Сводная матрица покрытия

Матрица — единый индекс проверок: `свойство -> dimension -> тест -> файл реализации`. Порядок соответствует пути данных. Подробности и диагностика приведены в карточках подкаталогов.

## Граница `CSV -> ODS`: `test_csv_ods`

| Проверяемое свойство | Dimension | Тест | Файл |
|---|---|---|---|
| Заголовок CSV соответствует контракту по составу и порядку колонок | Validity | `csv_header_matches_contract` | `src/validation/csv_ods_check.py` |
| Для пары `source_file`, `batch_date` есть запись журнала со статусом `loaded` | Completeness | `ods_batch_journal_loaded` | Там же |
| Состояние журнала соответствует требованиям загрузки | Completeness | `journal_batch_loaded` | Там же |
| Число строк ODS равно `journal.rows_loaded` | Consistency | `ods_rows_match_journal` | Там же |
| Число отклонённых строк не превышает порог | Accuracy | `ods_rejects_within_threshold` | Там же |
| `event_time` находится в границах суток batch | Validity | `ods_batch_time_bounds` | Там же |

## Граница `ODS -> Core`: `test_ods_core`

| Проверяемое свойство | Dimension | Тест | Файл |
|---|---|---|---|
| Число строк Core равно числу уникальных событий ODS за сутки batch | Consistency | `ods_core_rows_reconciliation` | `src/validation/integration_check.py` |
| `event_id` уникален | Uniqueness | `core_event_id_uniqueness` | Там же |
| Нет отрицательных цен | Validity | `core_negative_prices` | Там же |
| Минимальный и максимальный `event_time` в Core совпадают с ODS | Consistency | `ods_core_time_bounds` | Там же |
| Распределение `event_type` в Core совпадает с ODS | Consistency | `ods_core_event_type_distribution` | Там же |
| `event_type` принадлежит допустимому множеству | Validity | `core_allowed_event_types` | Там же |
| `event_date` соответствует `_batch_date` | Validity | `core_event_date_matches_batch` | Там же |
| `event_time` находится в границах суток batch | Validity | `core_batch_time_bounds` | Там же |
| Batch не найден или БД недоступна | Consistency | `ods_core_integration_check` | Там же |

Последняя строка описывает диагностический кейс раннего сбоя, а не обычную проверку свойства строк.

## Core: `fact_events`

| Проверяемое свойство | Dimension | Тест | Файл |
|---|---|---|---|
| Четыре ключевых поля заполнены | Completeness | `not_null` | `dbt/models/core/schema.yml` |
| `event_id` уникален | Uniqueness | `unique` | Там же |
| Нет NULL в обязательных полях, отрицательных цен и недопустимых типов | Integrity | `core_integrity` | `dbt/tests/core_integrity.sql` |
| Batch-метаданные заполнены | Consistency | `core_reconciliation` | `dbt/tests/core_reconciliation.sql` |

## Измерения и факты

| Проверяемое свойство | Dimension | Тест | Файл |
|---|---|---|---|
| `dim_category`: ключ заполнен и уникален | Completeness, Uniqueness | `dim_category_required`, `dim_category_unique` | `dbt/tests/` |
| `dim_category`: поля категории согласованы с разбором `category_code` | Integrity | `dim_category_integrity` | `dbt/tests/` |
| `dim_product`: ключ заполнен и уникален | Completeness, Uniqueness | `dim_product_required`, `dim_product_unique` | `dbt/tests/` |
| `dim_user`: ключ уникален, даты валидны | Uniqueness, Validity | `dim_user_unique`, `dim_user_valid_dates` | `dbt/tests/` |
| `fact_purchases`: обязательные поля заполнены, цена допустима, `event_id` уникален | Completeness, Validity, Uniqueness | `fact_purchases_not_null`, `fact_purchases_positive_price`, `fact_purchases_unique` | `dbt/tests/` |
| Каждая покупка существует в Core как событие `purchase` | Integrity | `fact_purchases_integrity` | `dbt/tests/` |
| Число покупок совпадает с Core | Consistency | `reconciliation_core_purchases` | `dbt/tests/` |
| Время, длительность и метрики сессий валидны | Validity | `fact_sessions_validity` | `dbt/tests/` |
| События batch покрыты реконструированными сессиями | Consistency | `reconciliation_core_sessions` | `dbt/tests/` |

## Витрины

| Проверяемое свойство | Dimension | Тест | Файл |
|---|---|---|---|
| Ключевые поля витрин заполнены | Completeness | Набор `not_null` | `dbt/models/marts/schema.yml` |
| `session_id` уникален в `mart_sessions` | Uniqueness | `unique` | Там же |
| `funnel_stage` содержит четыре допустимых значения | Validity | `accepted_values` | Там же |
| Грейн четырёх витрин уникален | Uniqueness | `marts_grain` | `dbt/tests/marts_grain.sql` |
| Числовые метрики витрин не отрицательны | Accuracy | `marts_non_negative_metrics` | `dbt/tests/` |
| `stage_count` и `revenue` в `mart_funnel` не отрицательны | Accuracy | `mart_funnel_metrics` | `dbt/tests/` |
| Время и метрики в `mart_sessions` валидны | Validity | `mart_sessions_validity` | `dbt/tests/` |
| Агрегаты витрин согласованы с Core за batch | Consistency | `reconciliation_core_marts` | `dbt/tests/` |

## Подкаталоги

- [tests/gates/](gates/) — Python-гейты границ этапов.
- [tests/generic/](generic/) — dbt generic-тесты из `schema.yml`.
- [tests/singular/](singular/) — dbt singular-тесты по слоям Core, dimensions, facts и marts.