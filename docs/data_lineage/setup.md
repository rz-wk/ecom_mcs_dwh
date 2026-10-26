# Настройка с нуля

Порядок настройки OpenMetadata и связанных интеграций при первоначальном развёртывании или восстановлении после переустановки OM.

## Шаг 1. Сервисы и ingestion в OM

| Сервис | Тип | Подключение | Include-фильтр схем |
|---|---|---|---|
| `postgres_ods` | PostgreSQL | `localhost:5432`, база `ecom_mcs_dwh` | `ods` |
| `clickhouse_core` | ClickHouse | `localhost:8123`, база `ecom_mcs_dwh_core` | `ecom_mcs_dwh_core` |
| `clickhouse_marts` | ClickHouse | `localhost:8123`, база `ecom_mcs_dwh_marts` | `ecom_mcs_dwh_marts` |
| `csv_staging` | SQLite | Заглушка `/tmp/csv_staging.db`; ingestion не создавать | — |
| `superset` | Dashboard / Superset | `http://host.docker.internal:8088`, API-пользователь | См. шаг 5 |
| `airflow_openlineage` | Pipeline / Airflow | Настройка через OpenLineage-транспорт | — |

Обязательные условия:

- Для каждого ingestion-сервиса включён `Mark Deleted Entities`;
- Include-фильтры схем заданы явно. Без них коннектор может обнаружить таблицы обеих баз ClickHouse, что приводит к дубликатам сущностей и ошибочным связям. См. [overview.md](overview.md), принцип 6;
- Из контейнера OM внешние сервисы доступны через `host.docker.internal`; `localhost` внутри контейнера указывает на сам контейнер.

### Удаление дубликатов сущностей

Если дубликаты уже появились:

1. Исправить include-фильтры.
2. Повторно запустить ingestion, чтобы лишние сущности были помечены как удалённые (soft delete).
3. При необходимости удалить их окончательно (hard delete).

Пример API-запроса:

```http
DELETE /api/v1/tables/<id>?hardDelete=true&recursive=true
```

Список сущностей, включая помеченные как удалённые:

```http
GET /api/v1/tables?include=deleted
```

Перед hard delete необходимо убедиться, что выбран правильный объект и его удаление не затронет нужные сущности.

## Шаг 2. Airflow: транспорт OpenLineage

В `airflow.cfg`:

```ini
[openlineage]
execute_in_thread = true
namespace = airflow_openlineage
transport = {"type": "http", "url": "http://127.0.0.1:8585/api/v1/openlineage/", "endpoint": "lineage", "auth": {"type": "api_key", "api_key": "<OM token>"}}

[cosmos]
listeners = cosmos.listeners.task_instance_listener
```

Важные особенности:

- В текущей конфигурации провайдер использует транспорт из `airflow.cfg`; переменная окружения `OPENLINEAGE_URL` не заменяет эту настройку.
- После изменения секций необходимо перезапустить все процессы Airflow, чтобы конфигурация применилась.


## Шаг 3. Эмиттеры

Реализация находится в `src/lineage/`:

- `ol.py` — общий helper;
- `emit_csv.py` — CSV -> ODS;
- `emit_spark.py` — ODS -> Core;
- `emit_dbt.py` — граф dbt-моделей.

В текущей среде скрипты запускаются глобальным `python3.14`, где установлен `requests`. В виртуальном окружении Airflow этот пакет может отсутствовать, поэтому команда запуска должна использовать интерпретатор, в котором доступны необходимые зависимости.

Описание каждого эмиттера — в [emitters/](emitters/README.md). Проверка результата — в [procedures.md](procedures.md), раздел «Проверка здоровья».

## Шаг 4. Размещение в DAG

Цепочка задач приведена в [architecture.md](architecture.md). Каждый эмиттер вызывается отдельной Bash-задачей. Пример запуска CSV-эмиттера:

```python
f'cd {_PROJECT_ROOT} && {python} -m src.lineage.emit_csv '
f'--source-file "data/raw/{ds[:7]}.csv"'
```

Эмиттеры должны запускаться после этапов, фактические зависимости которых они описывают. Не изменять порядок задач без проверки зависимостей: например, `emit_csv_lineage` должен выполняться до `test_csv_ods`, поскольку CSV-сущность нужна для привязки тест-кейсов.

## Шаг 5. Superset-агент

1. В Superset создать API-пользователя с ролью Admin. Учётные данные указать в настройках сервиса `superset` в OM.

2. В настройках Superset Metadata ingestion задать фильтры:

   - Dashboard Filter Pattern Include: `^E-commerce Customer Journey$` — фильтр по заголовку дашборда, а не по его идентификатору.
   - Chart Filter Pattern Include: `^(10[5-9]|11[0-9]|12[0-3])$` — фильтр по идентификаторам чартов выбранного дашборда.
   - Data Model: без фильтра, чтобы датасеты были доступны для связывания с дашбордами.
   - Mark Deleted: включён для дашбордов, чартов и датасетов.

3. Проверить, что фильтр чартов включает все чарты, на которые ссылается выбранный дашборд.

Ошибка `chart instance not found` означает, что фильтр исключил один или несколько нужных чартов. В этом случае расширить паттерн и повторить ingestion.