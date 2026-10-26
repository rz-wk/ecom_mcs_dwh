# Singular-тесты витрин

Файлы: `dbt/tests/<имя>.sql`.

## `marts_grain` — Uniqueness

- **Проверка:** уникальность грейна четырёх витрин:
  - `mart_products` — дата × товар;
  - `mart_users` — дата × пользователь;
  - `mart_sales` — дата × категория × бренд;
  - `mart_funnel` — дата × категория × бренд × этап.
- Для `mart_sessions` используется generic-тест `unique(session_id)`.
- **Провал:** возможны дубли при инкрементальном обновлении.
- **Действие:** проверить очистку и идемпотентность модели, затем пересчитать витрину за затронутую дату.

## `marts_non_negative_metrics` — Accuracy

- **Проверка:** числовые метрики `mart_products`, `mart_users` и `mart_sales` неотрицательны, включая `purchasing_users`, `purchased_products` и `avg_purchase_price`.
- **Провал:** вероятна ошибка агрегации в SQL-модели витрины.

## `mart_funnel_metrics` — Accuracy

- **Проверка:** `stage_count` и `revenue` в `mart_funnel` неотрицательны.
- Проверка вынесена отдельно из-за бизнес-значимости воронки. `remove_from_cart` не входит в цепочку `purchase <= cart <= view`: это отдельное действие, а не последовательный этап.
- **Провал:** вероятна ошибка агрегации витрины.

## `mart_sessions_validity` — Validity

- **Проверка:** `session_end >= session_start`, `events_count > 0`, все агрегаты неотрицательны.
- **Провал:** возможен повторный прогон sessionization или дефект логики.
- **Действие:** проверить расчёт сессий и пересчитать витрину за затронутую дату.

## `reconciliation_core_marts` — Consistency

- **Проверка:** агрегаты `mart_funnel`, `mart_products`, `mart_users` и `mart_sales` за `batch_date` согласованы с агрегатами `fact_events` за тот же период: количества `view`, `cart`, `remove_from_cart`, `purchase` и выручка.
- `mart_sessions` не сверяется этим тестом: sessionization не имеет прямого построчного соответствия с событиями, а её покрытие проверяет `reconciliation_core_sessions`.
- **Провал:** витрина разошлась с Core; проверить SQL-модель и логику агрегации.
- **Действие:** устранить причину и пересчитать затронутую витрину за дату.
- Переменная `batch_date` передаётся из DAG. Без неё тест пропускается; в штатном рабочем прогоне она должна быть задана.
- Результат отправляется в OM таском `push_dbt_test_results` на `fact_events`, поскольку коннектор не создаёт кейс для теста с зависимостями `source` и `ref`.
