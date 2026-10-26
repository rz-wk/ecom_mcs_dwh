/*
    Тест корректности парсинга массивов и имен категорий в dim_categories.

    Проверяет:
        - что category_path и выделенные root/leaf имена соответствуют разбору category_code.

    Ожидаемый результат:
        0 строк.
*/

WITH parsed_categories AS (
    SELECT
        category_id,
        category_code,
        splitByChar('.', category_code) AS actual_path,
        category_path AS expected_path,
        category_root_name,
        category_leaf_name
    FROM {{ ref('dim_category') }}
    WHERE category_code IS NOT NULL AND category_code != ''
)
SELECT *
FROM parsed_categories
WHERE actual_path != expected_path
   OR category_root_name != actual_path[1]
   OR category_leaf_name != arrayElement(actual_path, -1)