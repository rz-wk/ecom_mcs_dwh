# Справочник

## Файлы реализации

| Файл | Назначение |
|---|---|
| `src/lineage/ol.py` | Общий helper: отправка OpenLineage-событий и регистрация CSV-таблиц |
| `src/lineage/emit_csv.py` | Ребро CSV -> `ods.events` |
| `src/lineage/emit_spark.py` | Ребро `ods.events` -> `fact_events` |
| `src/lineage/emit_dbt.py` | Рёбра dbt-графа на основе `manifest.json` |
| `dbt/target/manifest.json` | Источник графа dbt-моделей; обновляется задачей `refresh_dbt_manifest` |

## Настройки

| Расположение | Назначение |
|---|---|
| `airflow.cfg`, секция `[openlineage]` | Транспорт OpenLineage-событий в OM и namespace пайплайна |
| `airflow.cfg`, секция `[cosmos]` | Настройки Cosmos listener статусов задач |
| Конфигурация проекта: `om.api`, `om.lineage_url`, `om.token` | Адрес OM, URL lineage API и токен; переменные окружения `OM_API`, `OM_LINEAGE_URL`, `OM_TOKEN` имеют приоритет |
| `dbt/profiles.yml` | Профиль подключения dbt к ClickHouse |

Порт и протокол подключения необходимо сверять с конкретным клиентом. Настройки HTTP-подключения OM ingestion, профиль dbt и TCP-подключение через `clickhouse_driver` не взаимозаменяемы.

## Сервисы OpenMetadata

Сервисы управляются в интерфейсе OM: `Settings -> Services`.

Используются:

- `postgres_ods`;
- `clickhouse_core`;
- `clickhouse_marts`;
- `csv_staging`;
- `superset`;
- `airflow_openlineage`.

Типы сервисов, параметры подключения и фильтры описаны в [setup.md](setup.md), шаг 1.

## Сопутствующие артефакты

- Тест-кейсы и результаты проверок качества данных — [Data Quality](../data_quality/README.md).
- Скрипты `src/lineage/dq.py` и `src/lineage/emit_dbt_test_results.py` также находятся в каталоге `src/lineage/`, но относятся к публикации результатов Data Quality, а не к построению графа lineage.
- OpenLineage-события задач DAG и межтабличные рёбра — разные виды информации: первые описывают ход выполнения пайплайна, вторые — зависимости между сущностями.