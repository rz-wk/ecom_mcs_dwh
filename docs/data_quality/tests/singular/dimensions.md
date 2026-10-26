# Singular-тесты измерений

Файлы: `dbt/tests/<имя>.sql`.

## `dim_category_required` — Completeness

- **Проверка:** `category_id` заполнен.
- **Провал:** источник или модель сформировали пустой идентификатор.

## `dim_category_unique` — Uniqueness

- **Проверка:** `category_id` уникален.
- **Провал:** в справочнике есть дубли.
- **Действие:** проверить дедупликацию и incremental-логику модели.

## `dim_category_integrity` — Integrity

- **Проверка:** `category_path`, `category_root_name` и `category_leaf_name` согласованы с разбором `category_code` по точкам.
- **Провал:** логика разбора категорий изменилась или работает некорректно.
- **Действие:** сопоставить SQL модели `dim_category` и логику теста.

## `dim_product_required` — Completeness

- **Проверка:** `product_id` заполнен.
- **Провал:** источник или модель сформировали пустой идентификатор.

## `dim_product_unique` — Uniqueness

- **Проверка:** `product_id` уникален.
- **Провал:** дубли в измерении продукта.
- **Действие:** проверить incremental-логику и дедупликацию, как для `dim_category_unique`.

## `dim_user_unique` — Uniqueness

- **Проверка:** `user_id` уникален.
- **Провал:** возможны дубли при повторном бэкфилле или некорректном incremental-обновлении `dim_user`.

## `dim_user_valid_dates` — Validity

- **Проверка:** `first_seen` и `last_seen` не равны NULL; `first_seen <= last_seen`.
- **Провал:** даты истории пользователя сформированы некорректно.
- **Действие:** проверить расчёт и обновление пользовательской истории.