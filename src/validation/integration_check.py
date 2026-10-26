'''
Финальная интеграционная проверка DWH pipeline.

Проверяет согласованность batch между PostgreSQL ODS
и ClickHouse Core, а также базовые условия целостности Core.

Проверки (docs/data_quality/tests/gates/integration_check.md):

    1. ODS batch существует.
    2. Core batch существует.
    3. Количество строк Core соответствует количеству
       уникальных исходных событий ODS после дедупликации.
    4. Все event_id в Core уникальны.
    5. Core содержит только допустимые event_type.
    6. Временные границы Core соответствуют ODS.
    7. Количество строк по event_type согласовано с ODS
       после дедупликации.
    8. Нет отрицательных price.
    9. event_date каждой строки совпадает с _batch_date.
    10. event_time Core внутри суток batch.

Все проверки вычисляются, затем сравниваются одной пачкой.
Результаты пушатся в OM (best-effort), при наличии Failed —
единый raise.

Скрипт не изменяет данные.

:example:
`python -m src.validation.integration_check
    --batch-date 2019-10-01
    --source-file 2019-10.csv`
'''

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, time, timedelta, timezone

import psycopg
from clickhouse_driver import Client

from config import _conf
from src.lineage.dq import push, push_checks
from src.utils.logging import configure_logging
from src.utils.timer import Timer

log = logging.getLogger(__name__)


_VALID_EVENT_TYPES = (
    'view',
    'cart',
    'remove_from_cart',
    'purchase',
)

_SOURCE_COLUMNS = (
    'event_time',
    'event_type',
    'product_id',
    'category_id',
    'category_code',
    'brand',
    'price',
    'user_id',
    'user_session',
)

_ODS_TABLE = 'postgres_ods.ecom_mcs_dwh.ods.events'


def _parse_args() -> argparse.Namespace:
    '''
    Разбирает аргументы командной строки.
    '''

    parser = argparse.ArgumentParser(
        description='Финальная интеграционная проверка DWH pipeline.',
    )

    parser.add_argument(
        '--batch-date',
        type=date.fromisoformat,
        required=True,
        help='Дата batch в формате YYYY-MM-DD.',
    )

    parser.add_argument(
        '--source-file',
        required=True,
        help='Имя исходного файла.',
    )

    return parser.parse_args()


def _get_postgres_connection() -> psycopg.Connection:
    '''
    Создает подключение к PostgreSQL ODS.
    '''

    database = _conf.get('databases.postgres_ods')

    return psycopg.connect(
        host=database['host'],
        port=database['port'],
        dbname=database['database'],
        user=database['user'],
        password=database['password'],
    )


def _get_clickhouse_client() -> Client:
    '''
    Создает подключение к ClickHouse Core через TCP.
    '''

    database = _conf.get('databases.clickhouse_core')

    return Client(
        host=database['host'],
        port=database['port'],
        user=database['user'],
        password=database['password'],
        database=database['database'],
    )

def _as_utc(moment: datetime | None) -> datetime | None:
    '''
    Гарантирует aware-время в UTC.
    '''

    if moment is None or moment.tzinfo is not None:
        return moment

    return moment.replace(tzinfo=timezone.utc)

def _get_ods_batch_profile(
    connection: psycopg.Connection,
    batch_date: date,
) -> tuple[dict, dict[str, int]]:
    '''
    Показатели ODS batch после дедупликации.

    Один проход DISTINCT + ROLLUP: строка is_total=1 — итоги
    (rows/min/max), остальные — распределение event_type.
    Дедупликация соответствует логике PySpark: полное совпадение
    всех исходных полей события.

    Фильтр по суткам event_time включает pruning месячных партиций
    и индекс event_time; эквивалентность фильтру _batch_date
    гарантирована гейтом csv_ods_check (ods_batch_time_bounds:
    строки batch лежат в своих сутках UTC), выполняемым раньше.
    '''

    columns = ', '.join(_SOURCE_COLUMNS)

    lower = datetime.combine(batch_date, time.min, tzinfo=timezone.utc)
    upper = lower + timedelta(days=1)

    lower = _as_utc(lower)
    upper = _as_utc(upper)

    query = f'''
        SELECT
            event_type,
            COUNT(*) AS rows_count,
            MIN(event_time) AS min_event_time,
            MAX(event_time) AS max_event_time
        FROM (
            SELECT DISTINCT
                {columns}
            FROM ods.events
            WHERE event_time >= %(lower)s
              AND event_time < %(upper)s
        ) AS deduplicated
        GROUP BY event_type
    '''

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            {
                'lower': lower,
                'upper': upper,
            },
        )
        rows = cursor.fetchall()

    event_types: dict[str, int] = {
        event_type: rows_count
        for event_type, rows_count, _, _ in rows
    }

    # Итоги: сумма по группам, min/max — по строкам групп.
    # Для NULL event_type (недопустим по контракту, но устойчиво):
    # строка попадает в распределение, в итог не входит не может —
    # COUNT(*) считает и NULL-группу; пропускаем только min/max NULL.
    total_rows = sum(
        count for _, count, _, _ in rows
    )

    min_time = min(
        (m for _, _, m, _ in rows if m is not None), default=None,
    )

    max_time = max(
        (x for _, _, _, x in rows if x is not None), default=None,
    )

    stats = {
        'rows_count': total_rows,
        'min_event_time': min_time,
        'max_event_time': max_time,
    }

    if not rows or total_rows == 0:
        raise ValueError(
            f'ODS batch не найден: batch_date={batch_date}',
        )

    return stats, event_types

# Замена на `_get_ods_batch_profile`
# def _get_ods_stats(
#     connection: psycopg.Connection,
#     batch_date: date,
# ) -> dict:
#     '''
#     Рассчитывает контрольные показатели ODS после дедупликации.

#     Дедупликация соответствует логике PySpark:
#     полное совпадение всех исходных полей события.
#     '''

#     columns = ', '.join(_SOURCE_COLUMNS)

#     query = f'''
#         WITH deduplicated AS (
#             SELECT DISTINCT
#                 {columns}
#             FROM ods.events
#             WHERE _batch_date = %(batch_date)s
#         )
#         SELECT
#             COUNT(*) AS rows_count,
#             MIN(event_time) AS min_event_time,
#             MAX(event_time) AS max_event_time
#         FROM deduplicated
#     '''

#     with connection.cursor() as cursor:
#         cursor.execute(
#             query,
#             {'batch_date': batch_date},
#         )
#         row = cursor.fetchone()

#     if row is None or row[0] == 0:
#         raise ValueError(
#             f'ODS batch не найден: batch_date={batch_date}',
#         )

#     return {
#         'rows_count': row[0],
#         'min_event_time': row[1],
#         'max_event_time': row[2],
#     }

# Замена на `_get_ods_batch_profile`
# def _get_ods_event_types(
#     connection: psycopg.Connection,
#     batch_date: date,
# ) -> dict[str, int]:
#     '''
#     Возвращает распределение event_type после дедупликации.
#     '''

#     columns = ', '.join(_SOURCE_COLUMNS)

#     query = f'''
#         WITH deduplicated AS (
#             SELECT DISTINCT
#                 {columns}
#             FROM ods.events
#             WHERE _batch_date = %(batch_date)s
#         )
#         SELECT
#             event_type,
#             COUNT(*)
#         FROM deduplicated
#         GROUP BY event_type
#         ORDER BY event_type
#     '''

#     with connection.cursor() as cursor:
#         cursor.execute(
#             query,
#             {'batch_date': batch_date},
#         )
#         rows = cursor.fetchall()

#     return {
#         event_type: count
#         for event_type, count in rows
#     }


def _get_core_stats(
    client: Client,
    batch_date: date,
    source_file: str,
) -> dict:
    '''
    Возвращает контрольные показатели Core batch.
    '''

    query = '''
        SELECT
            count(),
            min(event_time),
            max(event_time),
            uniqExact(event_id),
            countIf(price < 0),
            countIf(event_date != _batch_date)
        FROM fact_events
        WHERE
            _source_file = %(source_file)s 
            AND _batch_date = %(batch_date)s
    '''

    row = client.execute(
        query,
        {
            'batch_date': batch_date,
            'source_file': source_file,
        },
    )[0]

    return {
        'rows_count': row[0],
        'min_event_time': row[1],
        'max_event_time': row[2],
        'unique_event_ids': row[3],
        'negative_prices': row[4],
        'wrong_event_dates': row[5],
    }


def _get_core_event_types(
    client: Client,
    batch_date: date,
    source_file: str,
) -> dict[str, int]:
    '''
    Возвращает распределение event_type в Core batch.
    '''

    rows = client.execute(
        '''
        SELECT
            event_type,
            count()
        FROM fact_events
        WHERE
            _source_file = %(source_file)s
            AND _batch_date = %(batch_date)s
        GROUP BY event_type
        ORDER BY event_type
        ''',
        {
            'batch_date': batch_date,
            'source_file': source_file,
        },
    )

    return {
        event_type: count
        for event_type, count in rows
    }


def _validate(
    ods_stats: dict,
    ods_event_types: dict[str, int],
    core_stats: dict,
    core_event_types: dict[str, int],
    batch_date: date,
) -> list[dict]:
    '''
    Сравнивает показатели ODS и Core, возвращает исходы всех проверок.
    '''

    lower = datetime.combine(batch_date, time.min)
    upper = lower + timedelta(days=1)

    lower = _as_utc(lower)
    upper = _as_utc(upper)

    has_rows = core_stats['rows_count'] > 0

    return [
        {
            'table': _ODS_TABLE,
            'name': 'ods_core_rows_reconciliation',
            'ok': core_stats['rows_count'] == ods_stats['rows_count'],
            'value': (
                f'ods={ods_stats["rows_count"]}, '
                f'core={core_stats["rows_count"]}'
            ),
            'dimension': 'Consistency',
        },
        {
            'table': _ODS_TABLE,
            'name': 'core_event_id_uniqueness',
            'ok': core_stats['unique_event_ids'] == core_stats['rows_count'],
            'value': (
                f'rows={core_stats["rows_count"]}, '
                f'unique_event_ids={core_stats["unique_event_ids"]}'
            ),
            'dimension': 'Uniqueness',
        },
        {
            'table': _ODS_TABLE,
            'name': 'core_negative_prices',
            'ok': core_stats['negative_prices'] == 0,
            'value': f'negative_prices={core_stats["negative_prices"]}',
            'dimension': 'Validity',
        },
        {
            'table': _ODS_TABLE,
            'name': 'ods_core_time_bounds',
            'ok': (
                core_stats['min_event_time'] == ods_stats['min_event_time']
                and core_stats['max_event_time'] == ods_stats['max_event_time']
            ),
            'value': (
                f'ods=[{ods_stats["min_event_time"]}, '
                f'{ods_stats["max_event_time"]}], '
                f'core=[{core_stats["min_event_time"]}, '
                f'{core_stats["max_event_time"]}]'
            ),
            'dimension': 'Consistency',
        },
        {
            'table': _ODS_TABLE,
            'name': 'ods_core_event_type_distribution',
            'ok': core_event_types == ods_event_types,
            'value': f'ods={ods_event_types}, core={core_event_types}',
            'dimension': 'Consistency',
        },
        {
            'table': _ODS_TABLE,
            'name': 'core_allowed_event_types',
            'ok': not (set(core_event_types) - set(_VALID_EVENT_TYPES)),
            'value': f'core_types={sorted(core_event_types)}',
            'dimension': 'Validity',
        },
        {
            'table': _ODS_TABLE,
            'name': 'core_event_date_matches_batch',
            'ok': core_stats['wrong_event_dates'] == 0,
            'value': f'wrong_event_dates={core_stats["wrong_event_dates"]}',
            'dimension': 'Validity',
        },
        {
            'table': _ODS_TABLE,
            'name': 'core_batch_time_bounds',
            'ok': (
                has_rows
                and core_stats['min_event_time'] >= lower
                and core_stats['max_event_time'] < upper
            ),
            'value': (
                f'min={core_stats["min_event_time"]}, '
                f'max={core_stats["max_event_time"]}, '
                f'bounds=[{lower}, {upper})'
                if has_rows
                else 'no rows in batch'
            ),
            'dimension': 'Validity',
        },
    ]


def main() -> None:
    '''
    Запускает финальную интеграционную проверку.
    '''
    configure_logging()

    args = _parse_args()

    with Timer('Db connection'):
        postgres = _get_postgres_connection()
        clickhouse = _get_clickhouse_client()

    pushed = False

    try:
        # ods_stats = _get_ods_stats(postgres, args.batch_date)
        # ods_event_types = _get_ods_event_types(postgres, args.batch_date)
        with Timer('Ods stats'):
            ods_stats, ods_event_types = _get_ods_batch_profile(
                postgres,
                args.batch_date,
            )
        with Timer('Core stats'):
            core_stats = _get_core_stats(
                clickhouse, args.batch_date, args.source_file,
            )
        with Timer('Core event_types'):
            core_event_types = _get_core_event_types(
                clickhouse, args.batch_date, args.source_file,
            )

        with Timer('_validate'):
            checks = _validate(
                ods_stats=ods_stats,
                ods_event_types=ods_event_types,
                core_stats=core_stats,
                core_event_types=core_event_types,
                batch_date=args.batch_date,
            )

        with Timer('Push checks'):
            push_checks(checks)
            pushed = True

        failures = [c for c in checks if not c['ok']]

        if failures:
            raise ValueError(
                'ODS -> Core checks failed: '
                + '; '.join(f'{c["name"]}: {c["value"]}' for c in failures)
            )

        log.info('ODS -> Core reconciliation passed')
        log.info('Core integrity checks passed')
        log.info(
            'Integration check completed successfully: '
            'batch_date=%s, source_file=%s, rows=%s',
            args.batch_date,
            args.source_file,
            f'{core_stats["rows_count"]:,}',
        )

    except Exception as error:
        log.exception('Integration check failed: %s', error)

        # Ранний сбой (batch не найден, БД недоступна): агрегированный
        # failed-кейс, чтобы провал был виден в OM.
        if not pushed:
            push(
                _ODS_TABLE,
                'ods_core_integration_check',
                False,
                str(error),
                dimension='Consistency',
            )

        raise

    finally:
        postgres.close()
        clickhouse.disconnect()
        Timer.report()


if __name__ == '__main__':
    main()