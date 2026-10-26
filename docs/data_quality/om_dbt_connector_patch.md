# Патч dbt-коннектора OpenMetadata

Патч исправляет два дефекта коннектора `openmetadata-ingestion`
версии 1.13.5. Без патча витрина неполна: у проходящих тестов нет
статусов Success, у определений нет измерений.

Патч находится в `patches/om_dbt_metadata.py` и копируется поверх
файла пакета. Любая установка или обновление пакета
`openmetadata-ingestion` затирает патч — после обновления пакета
патч нужно установить заново.

Файл в виртуальном окружении Airflow:

```text
<airflow>/venv/lib/python3.12/site-packages/metadata/ingestion/
source/database/dbt/metadata.py
```

## Что исправляет патч

### 1. Статусы успешных тестов

Проблема: 

- коннектор считал отсутствующее поле `message` признаком
«записи не из тестового прогона» и отбрасывал результат. У dbt
норма — когда `message` заполняется только при падении теста, у успешных
тестов оно пустое. В итоге все успешные статусы молча
отбрасывались, и в OM появлялись только провалы.

Решение: 

- признаком настоящей записи считается наличие отметки
времени «execute» в поле `timing`. Записи без неё — служебные записи
компиляции из `dbt run` — пропускаются, остальные (включая успешные)
отправляются в OM.

### 2. Измерения на определениях тестов

Проблема: 

- коннектор не заполняет поле `dataQualityDimension`, все
определения оказываются без измерения («Без измерений» в
интерфейсе).

Решение: 

- при создании определения измерение берётся из словаря
`_DBT_DIMENSION` — подстрока имени теста сопоставляется с
измерением. Словарь и таблица соответствия имён в
[naming.md](naming.md). Там же правила: новый тип теста назвать
по шаблону и добавить подстроку в словарь.

>Важное свойство: измерение проставляется только в момент создания
определения. Существующее определение патч не обновит — см.
раздел «Смена словаря».

## Установка и переустановка

После установки или обновления пакета openmetadata-ingestion:

```bash
cp patches/om_dbt_metadata.py \
  <airflow>/venv/lib/python3.12/site-packages/metadata/ingestion/source/database/dbt/metadata.py
```

Проверить, что патч действительно стоит (файл пакета совпадает с
копией):

```bash
diff <airflow>/venv/lib/python3.12/site-packages/metadata/ingestion/source/database/dbt/metadata.py \
     patches/om_dbt_metadata.py
```

Пустой вывод diff — патч стоит. Если после обновления пакета
измерения пропали у новых тестов — первым делом этот diff.

## Точечные правки

Правка 1. Приём успешных результатов. 

Заменить блок в функции `add_dbt_test_result`, в котором эвристика `if not dbt_test_result.message` молча выбросывает все успешные результаты. Т.е. если у проходящих dbt-тестов `message = null` (а у dbt это норма: message пишется только при падении) и все успешные результаты молча отбрасываются, и статусы в OM не появляются:

```python
# было: пропускать записи с пустым message

                # Skip compiled-only entries: `dbt run` includes test nodes in
                # run_results.json with status="success" but message=null since
                # no test SQL was executed. Real results always have a message.
                if not dbt_test_result.message:
                    logger.debug(
                        "Skipping compiled-only test result for '%s' (message is null).",
                        manifest_node.name,
                    )
                    return
```

на:

```python
# стало: пропускать только служебные записи компиляции —
# у них нет отметки времени execute

                # Skip compiled-only entries: `dbt run` includes test nodes in
                # run_results.json with status="success" but without an
                # "execute" timing entry, since no test SQL was executed.
                if not any(
                    t.name == "execute" for t in (dbt_test_result.timing or [])
                ):
                    logger.debug(
                        "Skipping compiled-only test result for '%s' (no execute timing).",
                        manifest_node.name,
                    )
                    return
```

Правка 2. Измерение. 

В функции `create_dbt_tests_definition`, внутри ветки `if not check_test_definition_exists:`, перед созданием запроса добавить словарь и вычисление, и передать `dataQualityDimension=_dim` в `CreateTestDefinitionRequest`. Поле `dataQualityDimension` из словаря `_DBT_DIMENSION` — ищет подстроку в имени теста `dimension (Completeness/Validity/Uniqueness/Integrity/Consistency/Accuracy)`. Нет совпадения, значит `None (NoDimension)`:

```python
# Было
                if not check_test_definition_exists:
                    entity_type = EntityType.TABLE
                    if get_manifest_column_name(manifest_node):
                        entity_type = EntityType.COLUMN
                    yield Either(
                        right=CreateTestDefinitionRequest(
                            name=manifest_node.name,
                            description=manifest_node.description,
                            entityType=entity_type,
                            testPlatforms=[TestPlatform.dbt],
                            parameterDefinition=create_test_case_parameter_definitions(
                                manifest_node
                            ),
                            displayName=None,
                            owners=None,
                        )
                    )

```

```python
# Стало

                if not check_test_definition_exists:
                    entity_type = EntityType.TABLE
                    if get_manifest_column_name(manifest_node):
                        entity_type = EntityType.COLUMN
                    _DBT_DIMENSION = {
                          "not_null": "Completeness",
                          "accepted_values": "Validity",
                          "integrity": "Integrity",
                          "reconciliation": "Consistency",
                          "_grain": "Uniqueness",
                          "unique": "Uniqueness",
                          "valid_dates": "Validity",
                          "required": "Completeness",
                          "positive": "Validity",
                          "validity": "Validity",
                          "metrics": "Accuracy",
                          "_idempotency": "Accuracy",
                    }
                    _dim = next((v for p, v in _DBT_DIMENSION.items() if p in manifest_node.name), None)
                    yield Either(
                        right=CreateTestDefinitionRequest(
                            name=manifest_node.name,
                            description=manifest_node.description,
                            entityType=entity_type,
                            testPlatforms=[TestPlatform.dbt],
                            parameterDefinition=create_test_case_parameter_definitions(
                                manifest_node
                            ),
                            dataQualityDimension=_dim,
                            displayName=None,
                            owners=None,
                        )
                    )
```

## Смена словаря

Измерение проставляется только при создании определения, поэтому
правка словаря не действует на существующие определения. Порядок
обновления:

1. Внести правку словаря в `patches/om_dbt_metadata.py` и
   установить патч (раздел выше).
2. Удалить все кейсы — скрипт ниже. Это удаляет и кейсы
   Python-гейтов: после пересоздания выполнить гейты (прогон задач
   `test_csv_ods` и `test_ods_core` или скрипты вручную), они создадут
   свои кейсы заново.
3. Удалить все несистемные определения — второй скрипт.
4. Прогнать полный цикл: `refresh_dbt_manifest`, `process_dbt`,
   `ingest_dbt_tests`, `push_dbt_test_results`. Дешёвый перезапуск
   одних ingest-задач недопустим: коннектор прочитает устаревший
   `manifest` из общего каталога `target` и создаст удалённые тесты
   заново (`procedures.md`, раздел 3).

### Скрипт: удалить все кейсы

```bash
python3.14 - <<'PY'
import requests
OM = "http://127.0.0.1:8585/api/v1"
H = {"Authorization": "Bearer <token>"}
after = None
while True:
    p = {"limit": 100}
    if after:
        p["after"] = after
    r = requests.get(f"{OM}/dataQuality/testCases", params=p, headers=H).json()
    for tc in r.get("data", []):
        x = requests.delete(
            f"{OM}/dataQuality/testCases/{tc['id']}",
            params={"hardDelete": "true"},
            headers=H,
        )
        print(x.status_code, tc["fullyQualifiedName"])
    after = (r.get("paging") or {}).get("after")
    if not after or not r.get("data"):
        break
PY
```

### Скрипт: удалить несистемные определения

```bash
python3.14 - <<'PY'
import requests
OM = "http://127.0.0.1:8585/api/v1"
H = {"Authorization": "Bearer <token>"}
after = None
while True:
    p = {"limit": 100}
    if after:
        p["after"] = after
    r = requests.get(
        f"{OM}/dataQuality/testDefinitions", params=p, headers=H
    ).json()
    for d in r.get("data", []):
        if d.get("provider") == "system":
            continue
        x = requests.delete(
            f"{OM}/dataQuality/testDefinitions/{d['id']}",
            params={"hardDelete": "true"},
            headers=H,
        )
        print(x.status_code, d["name"])
    after = (r.get("paging") or {}).get("after")
    if not after or not r.get("data"):
        break
PY
```

## Ограничения

- Тесты с `ref` и `source` в зависимостях (например, сверки
  reconciliation_*) не получают кейс от коннектора, поэтому результаты
  пушит скрипт `emit_dbt_test_results.py`
  ([openmetadata.md](openmetadata.md), третий путь).
- Измерение нельзя задать в `dbt yml через meta` — коннектор это
  поле не читает, единственный путь — словарь патча.
- При ошибке «Parameter Values doesn't match Test Definition
  Parameters» — кейсы из `create_dbt_test_case` ссылаются на
  определения, созданные коннектором; сверить имена определений.

## Проверка после установки патча

Прогнать полный цикл дага, затем:

```bash
curl -s -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:8585/api/v1/dataQuality/testCases?limit=100&fields=testCaseResult" \
  | python3.14 -c "import sys,json;d=json.load(sys.stdin)['data']; \
    print('total:',len(d)); \
    [print(x['fullyQualifiedName'],'->',(x.get('testCaseResult') or {}).get('testCaseStatus')) for x in d[:10]]"
```

Ожидания: 

- число кейсов соответствует матрице покрытия (`tests/README.md`); 
- у проверенных кейсов статус `Success`; 
- в интерфейсе раздел `Data Dimensions` заполнен, «Без измерений» пуст.