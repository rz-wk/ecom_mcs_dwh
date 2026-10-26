# Контроль качества данных DWH: `ecom_mcs_dwh`

Система контроля качества проверяет данные на границах этапов и внутри DWH: от CSV-источника до аналитических витрин. При провале блокирующей проверки передача данных на следующие этапы останавливается.

Система включает три механизма:
- Python-гейты — контроль границ `CSV → ODS` и `ODS → Core`, включая сверки с журналом загрузки.
- dbt generic-тесты — декларативные проверки свойств колонок в `schema.yml`.
- dbt singular-тесты — проверки целостности таблиц, грейна, бизнес-инвариантов и согласованности слоёв.

Результаты проверок отображаются в OpenMetadata (OM). OM служит витриной качества, но не блокирует загрузку: блокировка реализована в Python-гейтах и `dbt build`.

![Data Health](../screenshots/om_data_quality_landing.png)

Рис. OM Data Quality: Total Tests, Healthy Data Assets, Data Assets Coverage

> Ключевое правило системы: одна проверка — один тест-владелец.

Каждое свойство данных закреплено за одним тестом. Дублирующие проверки исключаются, пробелы в покрытии выявляются. Полный список владельцев приведён в [сводной матрице](tests/README.md).

## Структура раздела

```text
docs/data_quality/
├── README.md                      
├── overview.md                    
├── architecture.md                
├── standards.md                   
├── naming.md                      
├── procedures.md                  
├── om_dbt_connector_patch.md      
├── openmetadata.md                
│
├── tests/
│   ├── README.md                  # сводная матрица проверок
│   │
│   ├── gates/                     # Python-гейты
│   │   ├── README.md
│   │   ├── csv_ods_check.md       # гейт CSV -> ODS
│   │   └── integration_check.md   # гейт ODS -> Core
│   │
│   ├── generic/                   # dbt generic-тесты (schema.yml)
│   │   ├── README.md
│   │   ├── core.md                
│   │   └── marts.md               
│   │
│   └── singular/                  # dbt singular-тесты (dbt/tests/*.sql)
│       ├── README.md
│       ├── core.md                
│       ├── dimensions.md          
│       ├── facts.md               
│       └── marts.md               
│
├── reference.md                  
└── changelog.md                   
```

## Оглавление

**Система**

- [overview.md](overview.md) — назначение, принципы и границы системы. Читать первым.
- [architecture.md](architecture.md) — механизмы контроля, контрольные точки конвейера и передача результатов в OM.
- [standards.md](standards.md) — стандарт покрытия для новых потоков, таблиц и витрин.
- [naming.md](naming.md) — правила именования тестов и автоматического назначения dimension.
- [procedures.md](procedures.md) — практические процедуры: добавление проверки и диагностика сбоев.

**Тесты**

- [tests/README.md](tests/README.md) — сводная матрица проверок: что и чем покрыто.
- [tests/gates/](tests/gates/) — Python-гейты на границах этапов.
- [tests/generic/](tests/generic/) — dbt generic-тесты из `schema.yml`.
- [tests/singular/](tests/singular/) — dbt singular-тесты по слоям: Core, измерения, факты и витрины.

**Справочные материалы**

- [openmetadata.md](openmetadata.md) — интеграция с OM: API, способы передачи результатов, метрика покрытия и ограничения.
- [om_dbt_connector_patch.md](om_dbt_connector_patch.md) — патч dbt-коннектора OM.
- [reference.md](reference.md) — расположение файлов, конфигурация и связанные DAG-таски.
- [changelog.md](changelog.md) — история изменений системы.

## Быстрые сценарии

- «Чем покрыта таблица X или свойство?» -> [tests/README.md](tests/README.md).
- «Гейт завершился с ошибкой. Что делать?» -> [procedures.md](procedures.md), раздел «Гейт упал».
- «Нужна проверка для новой витрины» -> [standards.md](standards.md), затем [procedures.md](procedures.md), раздел «Добавить проверку».
- «Что означает Failed в OM?» -> соответствующая карточка теста в каталоге [tests/](tests/README.md).