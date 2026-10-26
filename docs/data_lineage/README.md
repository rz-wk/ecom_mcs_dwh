# Сквозной Data Lineage

Data Lineage отвечает на два вопроса о любой таблице или витрине:

- откуда поступили данные;
- какие сущности и потребители от них зависят.

Граф в OpenMetadata (OM) объединяет зависимости на всём пути данных — от исходного CSV-файла до дашборда Superset.

![OM Explore Lineage](../screenshots/om_lineage_full_graph.png)

Рис. OM Explore Lineage для `mart_sales`: сквозной граф из восьми узлов от CSV до дашборда

### Как строится граф

Зависимости фиксируются четырьмя механизмами:

- Airflow OpenLineage provider — пайплайн, статусы запусков и задач;
- эмиттеры проекта — зависимости `CSV -> ODS`, `ODS -> Core` и граф dbt-моделей;
- Superset ingestion-агент OM — связи между таблицами, датасетами, чартами и дашбордами.

Подробности реализации, настройки и ограничения описаны в документах ниже.

Качество данных — тесты и статусы проверок — относится к отдельной системе. См. [Data Quality](../data_quality/README.md).

## Структура документации

```text
docs/data_lineage/
├── README.md
├── overview.md
├── architecture.md
├── setup.md
├── emitters/
│   ├── README.md
│   ├── ol.md
│   ├── csv.md
│   ├── spark.md
│   └── dbt.md
├── procedures.md
└── reference.md
```

## Оглавление

- [overview.md](overview.md) — назначение, итоговый граф и проверенные принципы. Читать первым;
- [architecture.md](architecture.md) — механизмы построения зависимостей, формат датасетов и размещение эмиттеров в DAG;
- [setup.md](setup.md) — настройка сервисов OM, OpenLineage-транспорта Airflow и Superset-агента с нуля;
- [emitters/](emitters/README.md) — назначение скриптов, их поведение и инструкции по расширению;
- [procedures.md](procedures.md) — сопровождение, диагностика типовых проблем и проверка графа после запуска;
- [reference.md](reference.md) — расположение файлов, настройки и связанные артефакты.

## Быстрые сценарии

- Проверить происхождение данных в `mart_*` — открыть Explore Lineage в OM или выполнить `curl` запрос из раздела «Проверка здоровья» в [procedures.md](procedures.md);
- Добавить ребро для нового этапа — изучить [emitters/README.md](emitters/README.md) и карточку подходящего эмиттера;
- Разобраться, почему после запуска не появились рёбра — обратиться к разделу «Типовые проблемы» в [procedures.md](procedures.md);
- Восстановить настройку OM — пройти шаги в [setup.md](setup.md).