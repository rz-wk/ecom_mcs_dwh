# Справочник

## Файлы системы

```text
src/validation/csv_ods_check.py      гейт CSV -> ODS
src/validation/integration_check.py  гейт ODS -> Core
src/lineage/dq.py                    отправка результатов Python-гейтов в OM
src/lineage/ol.py                    lineage-эмиттер и регистрация CSV-сущностей
src/lineage/emit_dbt_test_results.py отправка результатов dbt-тестов в OM в обход коннектора
src/ingestion/load_ods.py            загрузчик ODS: контракт, загрузка и журнал
dbt/models/core/schema.yml           generic-тесты Core и source
dbt/models/marts/schema.yml          generic-тесты витрин
dbt/tests/*.sql                      singular-тесты
dbt/ingestion/dbt_core.yml           конфигурация dbt-коннектора OM для Core
dbt/ingestion/dbt_marts.yml          конфигурация dbt-коннектора OM для витрин
patches/om_dbt_metadata.py           патч dbt-коннектора OM
```

## Конфигурация

| Ключ | Значение по умолчанию | Назначение |
|---|---|---|
| `validation.max_rows_rejected` | `0` | Допустимое число отклонённых строк за день для гейта `CSV -> ODS` |
| `om.api` / `om.token` | Не заданы в базовой конфигурации | Адрес OM и JWT; переменные окружения `OM_API` / `OM_TOKEN` |
| `databases.postgres_ods` / `databases.clickhouse_core` | Задаются конфигурацией окружения | Подключения Python-гейтов к PostgreSQL ODS и ClickHouse Core |

## Скрипты аудита

Аудит журнала против факта ODS и команды перезагрузки описаны в [procedures.md](procedures.md), раздел 2.

## Таски DAG

```text
test_csv_ods, test_ods_core                       Python-гейты
emit_csv_lineage, emit_spark_lineage,
emit_dbt_lineage                                  lineage
refresh_dbt_manifest -> process_dbt               Cosmos, dbt build / AFTER_ALL
ingest_dbt_tests                                  импорт результатов через dbt-коннектор OM
push_dbt_test_results                             отправка результатов сверок из белого списка в OM
```