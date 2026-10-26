'''
Проверка этапа «CSV -> PostgreSQL ODS» (гейт перед PySpark).

Проверки (docs/data_quality/tests/gates/csv_ods_check.md):

    1. `csv_header_matches_contract`  — заголовок csv = контракту
       _SOURCE_COLUMNS (состав и порядок: staging собирается
       позиционно);
    2. `ods_batch_journal_loaded`     — batch (source_file,
       batch_date) в журнале со статусом loaded;
    3. `journal_batch_loaded`         — журнал консистентен
       (loaded и rejects в допуске), носитель — `ingestion.journal`;
    4. `ods_rows_match_journal`       — count(ods.events) по batch
       равен `journal.rows_loaded`;
    5. `ods_rejects_within_threshold` — `rows_rejected` не выше порога
       `validation.max_rows_rejected` (default 0);
    6. `ods_batch_time_bounds`        — event_time внутри суток
       batch (UTC).

Результаты пушатся в OM (best-effort) на сущности
`csv_staging.raw.staging.<ГГГГ-ММ>`,
`postgres_ods.ecom_mcs_dwh.ods.events` и
`postgres_ods.ecom_mcs_dwh.ingestion.journal`.

Если CSV-файла нет (backfill по месяцу без данных) — молчаливый skip.
Скрипт не изменяет данные.

:example:
`python -m src.validation.csv_ods_check
    --batch-date 2019-10-01 --source-file data/raw/2019-10.csv`
'''

from __future__ import annotations

import argparse
import csv
import logging
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import psycopg

from config import _conf
# Контракт источника — единственный источник истины (load_ods).
from src.ingestion.load_ods import _SOURCE_COLUMNS
from src.lineage.dq import push_checks
from src.utils.logging import configure_logging
from src.utils.timer import Timer

log = logging.getLogger(__name__)

_ODS_TABLE = 'postgres_ods.ecom_mcs_dwh.ods.events'
_JOURNAL_TABLE = 'postgres_ods.ecom_mcs_dwh.ingestion.journal'
_CSV_TABLE = 'csv_staging.raw.staging.{stem}'


def _parse_args() -> argparse.Namespace:
    '''
    Разбирает аргументы командной строки.
    '''

    parser = argparse.ArgumentParser(
        description='Проверка этапа CSV -> PostgreSQL ODS.',
    )

    parser.add_argument(
        '--batch-date',
        type=date.fromisoformat,
        required=True,
        help='Дата batch в формате YYYY-MM-DD.',
    )

    parser.add_argument(
        '--source-file',
        type=Path,
        required=True,
        help='Путь к исходному CSV-файлу.',
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


def _journal_state(
    connection: psycopg.Connection,
    source_file: str,
    batch_date: date,
) -> tuple[str, int, int] | None:
    '''
    Возвращает (status, rows_loaded, rows_rejected) batch из журнала.
    '''

    query = '''
        SELECT status, rows_loaded, rows_rejected
        FROM ingestion.journal
        WHERE source_file = %s
          AND batch_date = %s
    '''

    with connection.cursor() as cursor:
        cursor.execute(query, (source_file, batch_date))
        return cursor.fetchone()


def _ods_rows_count(
    connection: psycopg.Connection,
    source_file: str,
    batch_date: date,
) -> int:
    '''
    Число строк ODS, загруженных этим batch.
    '''

    query = '''
        SELECT count(*)
        FROM ods.events
        WHERE _batch_date = %s
          AND _source_file = %s
    '''

    with connection.cursor() as cursor:
        cursor.execute(query, (batch_date, source_file))
        return cursor.fetchone()[0]


def _ods_time_bounds(
    connection: psycopg.Connection,
    source_file: str,
    batch_date: date,
) -> tuple[datetime | None, datetime | None]:
    '''
    Возвращает (min, max) event_time batch в ODS.
    '''

    query = '''
        SELECT MIN(event_time), MAX(event_time)
        FROM ods.events
        WHERE _batch_date = %s
          AND _source_file = %s
    '''

    with connection.cursor() as cursor:
        cursor.execute(query, (batch_date, source_file))
        return cursor.fetchone()


def _read_header(path: Path) -> list[str]:
    '''
    Читает заголовок csv (первая строка).
    '''

    with path.open(newline='') as f:
        return next(csv.reader(f))


def _as_utc(moment: datetime | None) -> datetime | None:
    '''
    Гарантирует aware-время в UTC.

    Наивное значение трактуется как UTC: семантика ODS — UTC
    (сессия загрузки зафиксирована в +00). Защита от разницы
    поведения драйвера (aware/naive) при сравнении границ.
    '''

    if moment is None or moment.tzinfo is not None:
        return moment

    return moment.replace(tzinfo=timezone.utc)


def main() -> None:
    '''
    Запускает проверку этапа CSV -> ODS.
    '''

    configure_logging()

    args = _parse_args()

    path = args.source_file

    if not path.exists():
        log.info('CSV not found, skip: %s', path)
        return

    with Timer('Get CSV table'):
        csv_table = _CSV_TABLE.format(stem=path.stem)

    checks: list[dict] = []

    # 1. Заголовок == контракт (включая порядок колонок).
    try:
        with Timer('Read header'):
            header = _read_header(path)

        checks.append({
            'table': csv_table,
            'name': 'csv_header_matches_contract',
            'ok': header == list(_SOURCE_COLUMNS),
            'value': f'csv={header}',
            'dimension': 'Validity',
        })

    except Exception as error:
        checks.append({
            'table': csv_table,
            'name': 'csv_header_matches_contract',
            'ok': False,
            'value': f'header read failed: {error}',
            'dimension': 'Validity',
        })

    # 2-5. Журнал и факт ODS.
    with Timer('Db connection'):
        postgres = _get_postgres_connection()

    try:
        with Timer('Journal state'):
            journal = _journal_state(postgres, path.name, args.batch_date)

        if journal is None:
            no_row = (
                f'no journal row: source_file={path.name}, '
                f'batch_date={args.batch_date}'
            )

            checks.append({
                'table': _ODS_TABLE,
                'name': 'ods_batch_journal_loaded',
                'ok': False,
                'value': no_row,
                'dimension': 'Completeness',
            })

            checks.append({
                'table': _JOURNAL_TABLE,
                'name': 'journal_batch_loaded',
                'ok': False,
                'value': no_row,
                'dimension': 'Completeness',
            })

        else:
            status, rows_loaded, rows_rejected = journal

            checks.append({
                'table': _ODS_TABLE,
                'name': 'ods_batch_journal_loaded',
                'ok': status == 'loaded',
                'value': f'status={status}',
                'dimension': 'Completeness',
            })

            with Timer('Ods rows count'):
                actual_rows = _ods_rows_count(postgres, path.name, args.batch_date)

            checks.append({
                'table': _ODS_TABLE,
                'name': 'ods_rows_match_journal',
                'ok': actual_rows == rows_loaded,
                'value': f'journal={rows_loaded}, fact={actual_rows}',
                'dimension': 'Consistency',
            })

            max_rejects = int(_conf.get('validation.max_rows_rejected', 0))

            checks.append({
                'table': _ODS_TABLE,
                'name': 'ods_rejects_within_threshold',
                'ok': (rows_rejected or 0) <= max_rejects,
                'value': f'rejected={rows_rejected}, max={max_rejects}',
                'dimension': 'Accuracy',
            })

            checks.append({
                'table': _JOURNAL_TABLE,
                'name': 'journal_batch_loaded',
                'ok': (
                    status == 'loaded'
                    and (rows_rejected or 0) <= max_rejects
                ),
                'value': f'status={status}, rejected={rows_rejected}',
                'dimension': 'Completeness',
            })

        # 6. Границы времени: строки batch принадлежат своим суткам UTC.
        # Вычисляется независимо от журнала. Пустой batch — границы
        # пусты, проверка истинна по определению.
        lower = datetime.combine(
            args.batch_date, time.min, tzinfo=timezone.utc,
        )
        upper = lower + timedelta(days=1)

        min_time, max_time = _ods_time_bounds(
            postgres, path.name, args.batch_date,
        )

        min_time = _as_utc(min_time)
        max_time = _as_utc(max_time)

        if min_time is None:
            bounds_ok = True
            bounds_value = 'no rows in batch'

        else:
            bounds_ok = min_time >= lower and max_time < upper
            bounds_value = f'min={min_time}, max={max_time}'

        checks.append({
            'table': _ODS_TABLE,
            'name': 'ods_batch_time_bounds',
            'ok': bounds_ok,
            'value': bounds_value,
            'dimension': 'Validity',
        })

    finally:
        postgres.close()

    with Timer('Push checks'):
        push_checks(checks)

    failures = [c for c in checks if not c['ok']]

    if failures:
        raise ValueError(
            'CSV -> ODS checks failed: '
            + '; '.join(f'{c["name"]}: {c["value"]}' for c in failures)
        )

    log.info(
        'CSV -> ODS checks passed: batch_date=%s, source_file=%s',
        args.batch_date,
        path.name,
    )

    Timer.report()


if __name__ == '__main__':
    main()