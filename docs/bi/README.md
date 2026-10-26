# BI layer — Apache Superset

## Назначение

BI-слой проекта реализован на Apache Superset и предназначен для анализа Customer Journey в e-commerce.

Superset используется для построения дашборда поверх подготовленных витрин ClickHouse.

>Логика подготовки и агрегации данных реализована на предыдущих слоях DWH. В BI выполняются визуализация, фильтрация и аналитическое представление данных.

## Источник данных

Superset запускается как отдельный BI-компонент проекта, подключается к ClickHouse и работает с готовыми витринами схемы `ecom_mcs_dwh_marts`.

## Datasets

Доступные datasets:

| Dataset         | Назначение                            |
| --------------- | ------------------------------------- |
| `mart_funnel`   | Этапы воронки и их динамика           |
| `mart_sessions` | Поведение и характеристики сессий     |
| `mart_products` | Эффективность продуктов               |
| `mart_users`    | Пользовательская активность и продажи |
| `mart_sales`    | Продажи, категории и бренды           |

>BI-слой использует только подготовленные marts и не обращается напрямую к ODS или Core.

Подробное описание datasets:

* [`datasets/mart_funnel.md`](datasets/mart_funnel.md)
* [`datasets/mart_sessions.md`](datasets/mart_sessions.md)
* [`datasets/mart_products.md`](datasets/mart_products.md)
* [`datasets/mart_users.md`](datasets/mart_users.md)
* [`datasets/mart_sales.md`](datasets/mart_sales.md)


## Dashboard

Основной dashboard — E-commerce Customer Journey

>Цель — показать Customer Journey от общего состояния до коммерческой детализации.

Аналитическая логика:

`Business outcome` -> `Funnel` -> `User behavior` -> `Commercial detail`

Визуальная иерархия:

`KPI` -> `trend` -> `comparison` -> `detail`

Включает 4 аналитические секции:

1. Обзор (трафик) — ключевые KPI и динамика.
2. Воронка и конверсия — движение пользователей по этапам customer journey.
3. Сессии и пользователи — поведение пользователей и качество сессий.
4. Продукты, категории и продажи — коммерческая детализация.

Содержит 18 графиков, использует Native Filters для интерактивной фильтрации данных.

![E-commerce Customer Journey](../screenshots/e-commerce-customer-journey.png)

Подробнее [`dashboards/overview.md`](dashboards/overview.md)


## Фильтрация и взаимодействие

Для анализа данных в различных временных и категориальных срезах настроены:

- Native Filters;
- область действия фильтров (scoping);
- cross-filtering.

Cross-filtering используется для интерактивного перехода от агрегированных показателей к детализации.


Основные Native фильтры:

- Период;
- Категория;
- Бренд;
- Продукт.

Применяются с учётом области действия конкретной секции:

- Обзор (трафик) — период;
- Воронка и конверсия — период, категория, бренд;
- Сессии и пользователи — период;
- Продукты, категории и продажи — период, категория, бренд, продукт.



## Структура документации

```text
superset/
├── README.md
├── datasets/
│   ├── mart_funnel.md
│   ├── mart_sessions.md
│   ├── mart_products.md
│   ├── mart_users.md
│   └── mart_sales.md
└── dashboards/
    └── overview.md
```
