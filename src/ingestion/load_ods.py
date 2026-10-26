'''
Загрузка событийных данных в PostgreSQL ODS.

Конвейер из трёх стадий:

    1. SCAN  — один потоковый проход по месячному CSV (pyarrow):
       жёсткая строковая схема (исключает скрытую деградацию
       int -> float при пропусках), маршрутизация по дню — сравнением
       префикса `YYYY-MM-DD` сырого `event_time`, до тяжёлого
       парсинга дат; правила валидации применяются только к строкам
       целевого периода; порядок колонок источника сверяется
       с контрактом на первом чанке;
    2. SPILL — валидные строки пишутся посуточно в staging-файлы CSV;
       строки, нарушившие контракт типов, попадают в карантин
       (`rejects`) с именем первого нарушенного правила; batch
       не прерывается; каталог стейджинга очищается на старте;
    3. LOAD  — пул воркеров (соединение на воркер): advisory-блокировка
       дня, COPY буферами через `copy.write()`,
       запись в журнал; удаление ранее загруженных batch — одним bulk-проходом до пула; staging- и reject-файлы дня удаляются после
       успешной загрузки.

Не делает дедупликацию и бизнес-трансформацию.
Они выполняются на последующих этапах pipeline.

Идемпотентность: состояние batch определяется по журналу
`ingestion.journal`; ранее загруженный batch (статус `loaded`)
удаляется и загружается заново. Точка возобновления после сбоя —
журнал; staging-файлы промежуточны и безопасно пересоздаются.

Режим resume (`--resume`): дни со статусом `loaded` в журнале
пропускаются, заливаются только остальные. Перезагрузка месяца
с нуля — предварительная чистка журнала по source_file.

Контракт схемы конкретной таблицы собран декларативно в одном месте
(`_SOURCE_COLUMNS`, `_DB_COLUMNS`, `_VALIDATION_RULES`) — универсализация
под список источников выполняется позже без переписывания движка.

Семантика времени: дневной batch определяется строковой датой источника;
`event_time` интерпретируется как UTC (сессия загрузки и литералы границ
партиций зафиксированы в `+00`). Строка с локальным офсетом может
относиться к соседнему календарному месяцу в UTC, поэтому партиции
создаются для месяцев целевых дней ±1 день, и удаление batch выполняется
по тому же расширенному множеству партиций. Пустые `category_code` /
`brand` / `user_session` загружаются как NULL. Источник ожидается в UTF-8
(единственная кодировка движка pyarrow).

Требования к окружению:
    - psycopg >= 3.1: `cursor.rowcount` после COPY;
    - pyarrow: контекстный менеджер `open_csv` (актуальные версии).

:example:
`python -m src.ingestion.load_ods --source-file data/raw/2019-10.csv --batch-date 2019-10-01` //=>
`2026-08-14 22:45:00 | INFO | __main__ | Batch completed: ... rows_loaded=1,234,567`

`python -m src.ingestion.load_ods --source-file data/raw/2019-10.csv --batch-date 2019-10-15 --load-mode month` //=>
`все дни октября загружены одним проходом по файлу (полный бэкфилл)`
'''

from __future__ import annotations

import argparse
import logging
import re
import shutil
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import psycopg
from psycopg import sql

from config import _conf
from src.utils.logging import configure_logging


log = logging.getLogger(__name__)


# =========================================================================
# Контракт схемы ODS.
# Единственное место модуля, описывающее конкретную таблицу: колонки
# источника, целевые колонки, обязательность, правила валидации.
# ========================================================================

_SOURCE_COLUMNS = [
    'event_time',
    'event_type',
    'product_id',
    'category_id',
    'category_code',
    'brand',
    'price',
    'user_id',
    'user_session',
]

_DB_COLUMNS = [
    *_SOURCE_COLUMNS,
    '_ingested_at',
    '_source_file',
    '_batch_date',
]

# Колонки с NOT NULL в DDL: пропуск значения — карантин, иначе COPY
# оборвёт весь batch посреди транзакции.
_REQUIRED_COLUMNS = frozenset({
    'event_time',
    'event_type',
    'product_id',
    'category_id',
    'price',
    'user_id',
})

# Опциональные колонки: пустая строка источника == NULL (контракт ODS).
# Замена на null обязательна до SPILL: CSV-аппендер пишет пустую строку
# как "" (в кавычках), а COPY с NULL '' читает "" как значение, что
# роняет типизированные колонки (uuid).
_OPTIONAL_EMPTY_AS_NULL = ('category_code', 'brand', 'user_session')

# Источник читается как строки и уходит в staging дословно: числовые
# поля и даты парсит PostgreSQL на этапе COPY. Валидация здесь нужна
# ровно для одного — заранее отбраковать строки, которые COPY не примет.
# Шаблоны заякорены: неанкоренный поиск допустил бы мусор вида `123abc`.
# Допускается необязательный хвост таймзоны (например, ` UTC`),
# который timestamptz парсит штатно.
_TIMESTAMP_PATTERN = re.compile(
    r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    r'(\.\d{1,6})?([+-]\d{2}:?\d{2}|[Zz]| [A-Za-z]{1,6})?$'
)
_INT_PATTERN = re.compile(r'^[+-]?\d+$')

# Дробная часть ограничена двумя знаками по NUMERIC(12,2): иначе COPY
# молча округлил бы значение — искажение данных без всякого следа.
_PRICE_PATTERN = re.compile(r'^[+-]?\d{1,10}(\.\d{1,2})?$')

_UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

_EVENT_TYPE_MAX_LENGTH = 16  # соответствует VARCHAR(16) в DDL.


def _build_validation_rules() -> dict[str, Callable[[pa.Table], pa.Array]]:
    '''
    Строит набор правил валидации колоночных данных.

    Каждое правило принимает таблицу строк и возвращает булев массив
    без null'ов: True — значение допустимо. Порядок правил определяет
    приоритет причины при карантине: фиксируется первое нарушение.

    :return dict[str, Callable]: Имя правила -> функция проверки.

    :example:
    `_build_validation_rules()['price_format'](table)` //=>
    `BooleanArray длины num_rows(table)`
    '''

    def required(name: str) -> Callable[[pa.Table], pa.Array]:
        # Обязательная колонка: значение присутствует и не пустое.
        def rule(table: pa.Table) -> pa.Array:
            column = table[name].fill_null('')

            return pc.greater(pc.utf8_length(column), 0)

        return rule

    def matches(name: str, pattern: re.Pattern) -> Callable[[pa.Table], pa.Array]:
        # Пустая строка не проходит шаблон: для обязательной колонки
        # причина уже назначена правилом required, для опциональной
        # пустота трактуется отдельно (см. optional_empty).
        def rule(table: pa.Table) -> pa.Array:
            column = table[name].fill_null('')

            return pc.match_substring_regex(column, pattern.pattern)

        return rule

    def optional_empty(name: str, pattern: re.Pattern) -> Callable[[pa.Table], pa.Array]:
        # Опциональная колонка: пустота эквивалентна NULL и допустима.
        def rule(table: pa.Table) -> pa.Array:
            column = table[name].fill_null('')

            empty = pc.equal(pc.utf8_length(column), 0)

            return pc.or_kleene(
                empty,
                pc.match_substring_regex(column, pattern.pattern),
            )

        return rule

    def max_length(name: str, limit: int) -> Callable[[pa.Table], pa.Array]:
        def rule(table: pa.Table) -> pa.Array:
            column = table[name].fill_null('')

            return pc.less_equal(pc.utf8_length(column), limit)

        return rule

    rules = {
        # Порядок = приоритет причин карантина.
        'event_time_missing': required('event_time'),
        'event_time_format': matches('event_time', _TIMESTAMP_PATTERN),
        'event_type_missing': required('event_type'),
        'event_type_length': max_length('event_type', _EVENT_TYPE_MAX_LENGTH),
        'product_id_missing': required('product_id'),
        'product_id_format': matches('product_id', _INT_PATTERN),
        'category_id_missing': required('category_id'),
        'category_id_format': matches('category_id', _INT_PATTERN),
        'price_missing': required('price'),
        'price_format': matches('price', _PRICE_PATTERN),
        'user_id_missing': required('user_id'),
        'user_id_format': matches('user_id', _INT_PATTERN),
        'user_session_format': optional_empty('user_session', _UUID_PATTERN),
    }

    return rules


_VALIDATION_RULES = _build_validation_rules()


# =========================================================================
# Конфигурация исполнения.
# Единая точка чтения настроек: отсутствующий ключ виден сразу на старте.
# =========================================================================

def _settings() -> dict:
    '''
    Собирает настройки загрузки из конфигурации.

    Fail-fast: недостающие ключи дают ошибку сразу, а не
    в середине многочасового прохода.

    :return dict: Настройки стадий конвейера.

    :example:
    `_settings()['copy_workers']` //=>
    `4`
    '''

    return {
        # Размер блока чтения pyarrow в байтах (ReadOptions.block_size).
        'read_block_bytes': int(_conf.get('ingestion.read_block_mb')) * 1024 * 1024,
        'staging_dir': Path(_conf.get('paths.staging')),
        'copy_workers': int(_conf.get('ingestion.copy_workers')),
        'copy_buffer_bytes': int(_conf.get('ingestion.copy_buffer_mb')) * 1024 * 1024,
        'connection': {
            'host': _conf.get('databases.postgres_ods.host'),
            'port': _conf.get('databases.postgres_ods.port'),
            'dbname': _conf.get('databases.postgres_ods.database'),
            'user': _conf.get('databases.postgres_ods.user'),
            'password': _conf.get('databases.postgres_ods.password'),
        },
    }


# =========================================================================
# Календарные вспомогательные функции.
# =========================================================================

def _month_range(
    year: int,
    month: int,
) -> tuple[date, date]:
    '''
    Границы календарного месяца: первый день месяца и первый день
    следующего.

    :param int year: Год.
    :param int month: Месяц.
    :return tuple[date, date]: Нижняя (включительно) и верхняя
        (исключительно) границы.

    :example:
    `_month_range(2019, 12)` //=>
    `(date(2019, 12, 1), date(2020, 1, 1))`
    '''

    lower = date(year, month, 1)

    next_year = year + (month == 12)
    next_month = month % 12 + 1

    upper = date(next_year, next_month, 1)

    return lower, upper


def _partition_name(
    month_first_day: date,
) -> str:
    '''
    Детерминированное имя месячной партиции.

    :param date month_first_day: Первый день месяца.
    :return str: Имя партиции.

    :example:
    `_partition_name(date(2019, 10, 1))` //=>
    `'events_y2019_m10'`
    '''

    return f'events_y{month_first_day.year}_m{month_first_day.month:02d}'


def _adjacent_months(
    days: set[date],
) -> list[tuple[int, int]]:
    '''
    Множество месяцев, затронутых целевыми днями с допуском ±1 день.

    Строка источника с локальным офсетом (например, `+03`) имеет
    префикс целевого дня, но в UTC может попасть в предыдущий
    или следующий календарный месяц — партиции и удаление batch
    обязаны покрывать эти месяцы.

    :param set[date] days: Целевые дни.
    :return list[tuple[int, int]]: Отсортированные пары (год, месяц).

    :example:
    `_adjacent_months({date(2019, 10, 1)})` //=>
    `[(2019, 9), (2019, 10), (2019, 11)]`
    '''

    months = {
        (shifted.year, shifted.month)
        for day in days
        for shifted in (
            day - timedelta(days=1),
            day,
            day + timedelta(days=1),
        )
    }

    return sorted(months)


def _target_days(
    load_mode: str,
    batch_date: date,
) -> set[date]:
    '''
    Множество целевых дат для выбранного режима загрузки.

    :param str load_mode: Режим `day` или `month`.
    :param date batch_date: Опорная дата (день либо представитель месяца).
    :return set[date]: Целевые дни.

    :example:
    `_target_days('month', date(2019, 10, 15))` //=>
    `{date(2019, 10, 1), ..., date(2019, 10, 31)}`
    '''

    if load_mode == 'day':
        return {batch_date}

    lower, upper = _month_range(batch_date.year, batch_date.month)

    days = []

    current = lower

    while current < upper:
        days.append(current)

        current += timedelta(days=1)

    return set(days)


# =========================================================================
# Staging-контур.
# =========================================================================

@dataclass
class ScanStats:
    '''
    Итоги стадии сканирования источника.

    :param int rows_total: Всего прочитано строк источника.
    :param int rows_outside_target: Строк вне целевого периода
        (в режиме day — все прочие дни месяца; в режиме month —
        только строки с нечитаемой датой).
    :param Counter quarantine: Карантин по причинам отбраковки.
    :param Counter day_rejected: Карантин по дням.
    :param dict[str, Path] day_files: День -> staging-файл.
    :param dict[str, Path] reject_files: День -> файл карантина.
    :param dict[str, int] day_rows: День -> число сплитуемых строк.
    '''

    rows_total: int = 0
    rows_outside_target: int = 0
    quarantine: Counter = field(default_factory=Counter)
    day_rejected: Counter = field(default_factory=Counter)
    day_files: dict[str, Path] = field(default_factory=dict)
    reject_files: dict[str, Path] = field(default_factory=dict)
    day_rows: dict[str, int] = field(default_factory=dict)


def _stage_dir(
    staging_root: Path,
    source_file: Path,
) -> Path:
    '''
    Каталог стейджинга для конкретного исходного файла.

    Имя файла участвует в пути: будущие источники с одинаковыми
    именами дней не конфликтуют по каталогам.

    :param Path staging_root: Корень области стейджинга.
    :param Path source_file: Исходный месячный CSV.
    :return Path: Каталог `<staging>/events/<имя файла без расширения>`.

    :example:
    `_stage_dir(Path('data/staging'), Path('data/raw/2019-10.csv'))` //=>
    `Path('data/staging/events/2019-10')`
    '''

    return staging_root / 'events' / source_file.stem


def _solid(
    mask: pa.Array | pa.ChunkedArray,
) -> pa.Array | pa.ChunkedArray:
    '''
    Приводит булев массив к маске без null'ов (null трактуется как False).

    :param Array | ChunkedArray mask: Исходный булев массив.
    :return Array | ChunkedArray: Маска без пропусков.

    :example:
    `_solid(pa.array([True, None]))` //=>
    `[True, False]`
    '''

    return mask.fill_null(False)

def _nullify_empty(
    table: pa.Table,
    columns: tuple[str, ...],
) -> pa.Table:
    '''
    Заменяет пустые строки на null в указанных колонках.

    Приводит данные к контракту «пустое == NULL» до записи стейджинга:
    null CSV-аппендер пишет пустым (незакавыченным) полем, что COPY
    корректно читает как NULL.

    :param pa.Table table: Таблица целевого периода.
    :param tuple[str, ...] columns: Опциональные колонки.
    :return pa.Table: Таблица с null вместо пустых строк.

    :example:
    `_nullify_empty(table, ('brand',))` //=>
    `пустые brand заменены на null`
    '''

    for name in columns:
        column = table[name].fill_null('')

        empty = pc.equal(pc.utf8_length(column), 0)

        table = table.set_column(
            table.schema.get_field_index(name),
            name,
            pc.if_else(
                empty,
                pa.scalar(None, pa.string()),
                column,
            ),
        )

    return table


# =========================================================================
# Стадия SCAN + SPILL.
# =========================================================================

def _evaluate_rules(
    table: pa.Table,
) -> tuple[pa.ChunkedArray, pa.Array]:
    '''
    Выполняет все правила валидации над таблицей строк.

    Возвращает комбинированную маску валидности и массив причин:
    для каждой невалидной строки фиксируется первое нарушенное
    правило, для валидных — null. Векторизация полностью в pyarrow:
    построчный Python-цикл отсутствует.

    :param pa.Table table: Чанк исходных строк (только целевой период).
    :return tuple[ChunkedArray, Array]: Маска валидности, причины.

    :example:
    `_evaluate_rules(chunk)` //=>
    `(BooleanArray, StringArray)`
    '''

    num_rows = table.num_rows

    combined = None
    reasons = pa.nulls(num_rows, type=pa.string())

    for rule_name, rule_fn in _VALIDATION_RULES.items():

        try:
            ok = _solid(rule_fn(table))

        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            # Колонка пришла с неожиданным физическим типом — вся
            # колонка считается нарушением данного правила.
            ok = pa.array([False] * num_rows, type=pa.bool_())

        combined = ok if combined is None else pc.and_kleene(combined, ok)

        # Причина фиксируется по первому несработавшему правилу:
        # строки с уже назначенной причиной не перезаписываются.
        unassigned = reasons.is_null()

        reasons = pc.if_else(
            pc.and_kleene(unassigned, pc.invert(ok)),
            pa.scalar(rule_name),
            reasons,
        )

    return combined, reasons


def _scan_source(
    source_file: Path,
    target_days: set[date],
    block_bytes: int,
    stage_dir: Path,
    ingested_at_iso: str,
) -> ScanStats:
    '''
    Один потоковый проход по месячному CSV с посуточным разбором.

    Все колонки читаются строго строками: типовой контракт источника
    фиксирован, деградация целых чисел к float при пропусках исключена.
    Маршрутизация: строки сначала фильтруются по целевому периоду
    (в режиме day отсекается ~96% чанка), правила валидации и раскладка
    по дням применяются только к уменьшенной таблице.

    На первом чанке порядок колонок источника сверяется с контрактом:
    staging-таблицы собираются позиционно, тихое перемешивание колонок
    недопустимо и останавливает загрузку явной ошибкой.

    Каталог стейджинга очищается перед проходом: файлы промежуточны
    по контракту, остатки прерванных запусков не должны накапливаться.

    :param Path source_file: Исходный месячный CSV.
    :param set[date] target_days: Целевые дни.
    :param int block_bytes: Размер блока чтения reader'а в байтах.
    :param Path stage_dir: Каталог стейджинга источника.
    :param str ingested_at_iso: Техническая метка времени загрузки.
    :return ScanStats: Статистика и пути созданных файлов.
    :raises RuntimeError: Если порядок колонок источника не совпадает
        с контрактом.

    :example:
    `_scan_source(csv, {date(2019,10,1)}, 32 << 20, stage, iso)` //=>
    `ScanStats(rows_total=..., day_files={'2019-10-01': Path(...)})`
    '''

    # Остатки прошлых запусков (в т.ч. других режимов) неактуальны:
    # SCAN пересоздаёт все файлы.
    if stage_dir.exists():
        shutil.rmtree(stage_dir)

    stage_dir.mkdir(parents=True, exist_ok=True)

    (stage_dir / 'rejects').mkdir(exist_ok=True)

    read_options = pacsv.ReadOptions(
        # ReadOptions.block_size измеряется БАЙТАМИ, а не строками:
        # малое значение дробит поток на десятки тысяч итераций
        # с полным набором векторных операций на каждой.
        block_size=block_bytes,
    )

    convert_options = pacsv.ConvertOptions(
        # Жёсткая типизация входа: каждая колонка — строка.
        column_types={name: pa.string() for name in _SOURCE_COLUMNS},
        # Аналог usecols: лишние колонки файла не читаются вовсе.
        include_columns=_SOURCE_COLUMNS,
    )

    stage_schema = pa.schema([
        *(pa.field(name, pa.string()) for name in _SOURCE_COLUMNS),
        pa.field('_ingested_at', pa.string()),
        pa.field('_source_file', pa.string()),
        pa.field('_batch_date', pa.string()),
    ])

    reject_schema = pa.schema([
        *(pa.field(name, pa.string()) for name in _SOURCE_COLUMNS),
        pa.field('_batch_date', pa.string()),
        pa.field('_reject_reason', pa.string()),
    ])

    target_prefixes = sorted(d.isoformat() for d in target_days)
    target_values = pa.array(target_prefixes)

    stats = ScanStats()

    day_writers: dict[str, pacsv.CSVWriter] = {}
    reject_writers: dict[str, pacsv.CSVWriter] = {}

    day_counts: Counter = Counter()

    schema_verified = False

    start = time.monotonic()

    def close_writers() -> None:
        # Ошибка закрытия одного writer'а не должна оставить открытыми
        # остальные: каждый закрывается независимо.
        for writer in (*day_writers.values(), *reject_writers.values()):
            try:
                writer.close()

            except Exception:
                log.exception('Staging writer close failed')

    try:
        with pacsv.open_csv(
            source_file,
            read_options=read_options,
            convert_options=convert_options,
        ) as reader:

            for table in reader:

                if not schema_verified:
                    # Сборка staging-таблиц позиционна: расхождение
                    # порядка колонок с контрактом означало бы тихую
                    # подмену данных. Ошибка конфигурации источника,
                    # а не повод для карантина — остановка загрузки.
                    if list(table.column_names) != _SOURCE_COLUMNS:
                        raise RuntimeError(
                            f'Порядок колонок источника не совпадает '
                            f'с контрактом: file={source_file}, '
                            f'columns={list(table.column_names)}',
                        )

                    schema_verified = True

                stats.rows_total += table.num_rows

                # День определяется префиксом сырой строки: самый
                # дешёвый маршрут, без парсинга дат на этой стадии.
                day_key = pc.utf8_slice_codeunits(
                    table['event_time'].fill_null(''),
                    start=0,
                    stop=10,
                )

                in_target = _solid(
                    pc.is_in(day_key, value_set=target_values),
                )

                stats.rows_outside_target += (
                    pc.sum(pc.invert(in_target)).as_py() or 0
                )

                # Правила валидации выполняются только на строках
                # целевого периода: вне-target не карантинятся.
                target_table = table.filter(in_target)

                if target_table.num_rows == 0:
                    continue

                # Контракт «пустое == NULL» — до правил и SPILL:
                # правила на null дают те же исходы (fill_null внутри).
                target_table = _nullify_empty(
                    target_table,
                    _OPTIONAL_EMPTY_AS_NULL,
                )

                target_day_key = day_key.filter(in_target)

                valid, reasons = _evaluate_rules(target_table)

                rejected_mask = pc.invert(valid)

                # --- SPILL валидных строк ---
                if pc.any(valid).as_py():
                    staged_table = target_table.filter(valid)

                    staged_days = target_day_key.filter(valid)

                    for day_str in staged_days.unique().to_pylist():
                        day_mask = pc.equal(staged_days, pa.scalar(day_str))

                        day_table = staged_table.filter(day_mask)

                        if day_str not in day_writers:
                            day_writers[day_str] = pacsv.CSVWriter(
                                str(stage_dir / f'{day_str}.csv'),
                                stage_schema,
                            )

                        day_rows_num = day_table.num_rows

                        stage_table = pa.table(
                            [
                                *day_table.columns,
                                # Технические поля — константные массивы
                                # длины конкретного дня: вне цикла их
                                # нельзя, длина должна совпадать с
                                # day_table (в month-режиме дней в чанке
                                # несколько).
                                pa.repeat(
                                    pa.scalar(ingested_at_iso),
                                    day_rows_num,
                                ),
                                pa.repeat(
                                    pa.scalar(source_file.name),
                                    day_rows_num,
                                ),
                                pa.repeat(
                                    pa.scalar(day_str),
                                    day_rows_num,
                                ),
                            ],
                            schema=stage_schema,
                        )

                        day_writers[day_str].write_table(stage_table)

                        day_counts[day_str] += day_rows_num

                # --- Карантин нарушивших контракт ---
                if pc.any(rejected_mask).as_py():
                    rejected_table = target_table.filter(rejected_mask)

                    rejected_days = target_day_key.filter(rejected_mask)

                    # Причины без null: строка карантина всегда имеет
                    # первое нарушенное правило; 'unknown' — защита.
                    rejected_reasons = reasons.filter(rejected_mask).fill_null(
                        'unknown',
                    )

                    # Счётчики из одного value_counts(): порядок записей
                    # гарантированно соответствует самим значениям.
                    for entry in rejected_days.value_counts().to_pylist():
                        stats.day_rejected[entry['values']] += entry['counts']

                    for entry in rejected_reasons.value_counts().to_pylist():
                        stats.quarantine[entry['values']] += entry['counts']

                    for day_str in rejected_days.unique().to_pylist():
                        day_mask = pc.equal(rejected_days, pa.scalar(day_str))

                        day_rejects = rejected_table.filter(day_mask)

                        if day_str not in reject_writers:
                            reject_writers[day_str] = pacsv.CSVWriter(
                                str(stage_dir / 'rejects' / f'{day_str}.csv'),
                                reject_schema,
                            )

                        reject_writers[day_str].write_table(
                            pa.table(
                                [
                                    *day_rejects.columns,
                                    pa.repeat(
                                        pa.scalar(day_str),
                                        day_rejects.num_rows,
                                    ),
                                    rejected_reasons.filter(day_mask),
                                ],
                                schema=reject_schema,
                            )
                        )

    finally:
        close_writers()

    stats.day_files = {
        day_str: stage_dir / f'{day_str}.csv'
        for day_str in day_writers
    }
    stats.reject_files = {
        day_str: stage_dir / 'rejects' / f'{day_str}.csv'
        for day_str in reject_writers
    }
    stats.day_rows = dict(day_counts)

    log.info(
        'Scan completed: rows_total=%s, outside_target=%s, rejected=%s, '
        'days=%s, elapsed=%.1fs',
        f'{stats.rows_total:,}',
        f'{stats.rows_outside_target:,}',
        f'{sum(stats.quarantine.values()):,}',
        len(stats.day_files),
        time.monotonic() - start,
    )

    return stats


# =========================================================================
# Журнал загрузок.
# =========================================================================

def _journal_running(
    connection: psycopg.Connection,
    source_file: str,
    batch_date: date,
) -> None:
    '''
    Регистрирует начало загрузки batch.

    UPSERT позволяет перезаписывать историю при повторной загрузке.

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param str source_file: Имя исходного файла.
    :param date batch_date: Дата batch.

    :example:
    `_journal_running(connection, '2019-10.csv', date(2019, 10, 1))` //=>
    `строка журнала со статусом running`
    '''

    query = '''
        INSERT INTO ingestion.journal
            (source_file, batch_date, status, started_at)
        VALUES (%s, %s, 'running', now())
        ON CONFLICT (source_file, batch_date)
        DO UPDATE SET
            status = 'running',
            rows_loaded = NULL,
            rows_rejected = NULL,
            started_at = EXCLUDED.started_at,
            finished_at = NULL,
            error = NULL
    '''

    with connection.cursor() as cursor:
        cursor.execute(query, (source_file, batch_date))


def _journal_finished(
    connection: psycopg.Connection,
    source_file: str,
    batch_date: date,
    rows_loaded: int,
    rows_rejected: int,
    error: str | None = None,
) -> str:
    '''
    Финализирует запись журнала успешной или неудачной загрузки.

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param str source_file: Имя исходного файла.
    :param date batch_date: Дата batch.
    :param int rows_loaded: Число загруженных строк.
    :param int rows_rejected: Число строк карантина.
    :param str | None error: Текст ошибки при статусе failed.
    :return str: Финальный статус batch.

    :example:
    `_journal_finished(connection, '2019-10.csv', date(2019, 10, 1), 1000, 0)` //=>
    `'loaded'`
    '''

    status = 'failed' if error else 'loaded'

    query = '''
        UPDATE ingestion.journal
        SET status = %s,
            rows_loaded = %s,
            rows_rejected = %s,
            finished_at = now(),
            error = %s
        WHERE source_file = %s
          AND batch_date = %s
    '''

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                status,
                rows_loaded,
                rows_rejected,
                error,
                source_file,
                batch_date,
            ),
        )

    return status

# =========================================================================
# Стадия LOAD.
# =========================================================================

def _ensure_partitions(
    connection: psycopg.Connection,
    target_days: set[date],
) -> None:
    '''
    Создаёт отсутствующие месячные партиции целевого периода.

    Покрытие — месяцы целевых дней с допуском ±1 день: строка с
    локальным офсетом имеет префикс целевого дня, но в UTC попадает
    в соседний месяц; без соседней партиции COPY оборвёт весь день.

    Выполняется в главной нити ДО запуска пула воркеров: параллельные
    `CREATE TABLE IF NOT EXISTS` одной партиции внутри процесса —
    известная гонка в PostgreSQL. Состязание между процессами
    обрабатывается явно: 42P07 (таблица уже создана конкурентом)
    трактуется как безвредный исход.

    Границы задаются строковыми литералами с явным смещением `+00`:
    bind-параметры в utility-командах запрещены, а явный офсет делает
    границы независимыми от серверного `TimeZone`.

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param set[date] target_days: Целевые дни.

    :example:
    `_ensure_partitions(connection, {date(2019, 10, 1)})` //=>
    `существуют ods.events_y2019_m09, ods.events_y2019_m10, ods.events_y2019_m11`
    '''

    for year, month in _adjacent_months(target_days):
        lower, upper = _month_range(year, month)

        query = sql.SQL(
            'CREATE TABLE IF NOT EXISTS ods.{} '
            'PARTITION OF ods.events '
            'FOR VALUES FROM ({}) TO ({})'
        ).format(
            sql.Identifier(_partition_name(lower)),
            sql.Literal(f'{lower.isoformat()} 00:00:00+00'),
            sql.Literal(f'{upper.isoformat()} 00:00:00+00'),
        )

        try:
            connection.execute(query)

        except psycopg.errors.DuplicateTable:
            # Партиция создана параллельным процессом между проверкой
            # IF NOT EXISTS и CREATE — целевое состояние достигнуто.
            # Aborted-транзакция откатывается, следующие команды
            # начинают её заново.
            connection.rollback()

        # На период заливки уборка отключается: autovacuum по мёртвым
        # строкам от bulk-DELETE конкурирует с COPY. Возврат — в
        # стадии indexes (05_maintenance.sql).
        connection.execute(
            sql.SQL('ALTER TABLE ods.{} SET (autovacuum_enabled = false)')
            .format(sql.Identifier(_partition_name(lower))),
        )

    connection.commit()


def _acquire_day_lock(
    connection: psycopg.Connection,
    source_file: str,
    batch_date: date,
) -> None:
    '''
    Захватывает сессионную advisory-блокировку дня.

    Защита от параллельного запуска двух процессов загрузки на одну
    дату: журнал сам по себе не сериализует конкурирующие процессы.

    Блокировка сессионная и освобождается закрытием соединения —
    в том числе при сбое: упавшая транзакция оставляет соединение
    в состоянии aborted, явный release оттуда невозможен.

    Ключ строится через hashtext(имя файла): теоретическая коллизия
    хеша двух разных файлов даст ложный «заблокировано другим
    процессом» — вероятность ничтожна, последствия безопасны.

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param str source_file: Имя исходного файла.
    :param date batch_date: Дата batch.
    :raises RuntimeError: Если блокировка уже удержана другим процессом.

    :example:
    `_acquire_day_lock(connection, '2019-10.csv', date(2019, 10, 1))` //=>
    `блокировка захвачена`
    '''

    # Классический двух- int32 ключ: hashtext(имя файла) + YYYYMMDD.
    lock_day = batch_date.year * 10_000 + batch_date.month * 100 + batch_date.day

    query = '''
        SELECT pg_try_advisory_lock(hashtext(%s), %s)
    '''

    with connection.cursor() as cursor:
        cursor.execute(query, (source_file, lock_day))

        acquired = cursor.fetchone()[0]

    if not acquired:
        raise RuntimeError(
            f'Batch заблокирован другим процессом: '
            f'source_file={source_file}, batch_date={batch_date}',
        )


def _loaded_batch_dates(
    connection: psycopg.Connection,
    source_file: str,
    target_days: set[date],
) -> list[date]:
    '''
    Возвращает целевые дни с успешным статусом в журнале.

    Читается один раз до пула воркеров: после UPSERT 'running'
    статус 'loaded' из журнала получить невозможно.

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param str source_file: Имя исходного файла.
    :param set[date] target_days: Целевые дни.
    :return list[date]: Отсортированные дни с зафиксированной загрузкой.

    :example:
    `_loaded_batch_dates(connection, '2019-10.csv', days)` //=>
    `[date(2019, 10, 1), date(2019, 10, 2)]`
    '''

    query = '''
        SELECT batch_date
        FROM ingestion.journal
        WHERE source_file = %s
          AND status = 'loaded'
          AND batch_date = ANY(%s)
    '''

    with connection.cursor() as cursor:
        cursor.execute(query, (source_file, sorted(target_days)))

        return sorted(row[0] for row in cursor.fetchall())


def _delete_batches_bulk(
    connection: psycopg.Connection,
    source_file: str,
    batch_dates: list[date],
) -> int:
    '''
    Удаляет строки перечисленных batch одним проходом на партицию (один seq scan на партицию на весь целевой период).

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param str source_file: Имя исходного файла.
    :param list[date] batch_dates: Дни ранее загруженных batch.
    :return int: Число удалённых строк по всем затронутым партициям.

    :example:
    `_delete_batches_bulk(connection, '2019-10.csv', [date(2019,10,1)])` //=>
    `1244245`
    '''

    if not batch_dates:
        return 0

    total_deleted = 0

    for year, month in _adjacent_months(set(batch_dates)):
        lower, _ = _month_range(year, month)

        query = sql.SQL(
            '''
            DELETE FROM ods.{}
            WHERE _source_file = {}
              AND _batch_date = ANY({})
            '''
        ).format(
            sql.Identifier(_partition_name(lower)),
            sql.Literal(source_file),
            sql.Literal(batch_dates),
        )

        with connection.cursor() as cursor:
            cursor.execute(query)

            total_deleted += cursor.rowcount

    return total_deleted


def _copy_stream(
    cursor: psycopg.Cursor,
    stage_file: Path,
    buffer_size: int,
) -> int:
    '''
    Потоковая загрузка staging-файла через COPY буферами.

    Вместо построчной сериализации копируются готовые буферы байтов —
    узкое место прежней реализации `write_row()` устранено. Экранирование
    формата CSV выполнено аппендером pyarrow на стадии SPILL.

    :param psycopg.Cursor cursor: Курсор соединения воркера.
    :param Path stage_file: Staging-файл дня.
    :param int buffer_size: Размер буфера передачи в байтах.
    :return int: Число загруженных строк.
    :raises RuntimeError: Если драйвер не отдал число скопированных строк
        (psycopg < 3.1 возвращает -1 — журнал был бы испорчен).

    :example:
    `_copy_stream(cursor, path, 32 * 1024 * 1024)` //=>
    `1244245`
    '''

    copy_query = sql.SQL(
        '''
        COPY ods.events ({columns})
        FROM STDIN
        WITH (
            FORMAT CSV,
            NULL '',
            HEADER true
        )
        '''
    ).format(
        columns=sql.SQL(', ').join(
            sql.Identifier(column)
            for column in _DB_COLUMNS
        ),
    )

    with cursor.copy(copy_query) as copy:
        with stage_file.open('rb') as payload:
            while True:
                chunk = payload.read(buffer_size)

                if not chunk:
                    break

                copy.write(chunk)

    rows_copied = cursor.rowcount

    if rows_copied < 0:
        raise RuntimeError(
            'COPY не вернул число строк: требуется psycopg >= 3.1',
        )

    return rows_copied


def _load_one_day(
    connection_params: dict,
    source_file: str,
    batch_date: date,
    stage_file: Path | None,
    reject_file: Path | None,
    rows_rejected: int,
    buffer_size: int,
) -> int:
    '''
    Полный цикл загрузки одного дня: собственное соединение,
    advisory-блокировка, журнальные транзакции, COPY.

    Предыдущее состояние batch читается ДО пометки `running`: после
    UPSERT статус уже не может быть `loaded`, и признак релоада
    из журнала получить невозможно.

    Очистка ранее загруженных batch выполняется bulk-проходом в main() до запуска пула.

    После успешной загрузки staging- и reject-файлы дня удаляются:
    пик занятия диска перестаёт расти в течение прохода. Повторный
    прогон безопасен — SCAN пересоздаёт файлы.

    :param dict connection_params: Параметры подключения.
    :param str source_file: Имя исходного файла.
    :param date batch_date: Дата batch.
    :param Path | None stage_file: Staging-файл дня (None — пустой день).
    :param Path | None reject_file: Файл карантина дня.
    :param int rows_rejected: Число строк карантина дня.
    :param int buffer_size: Размер буфера COPY в байтах.
    :return int: Число загруженных строк.

    :example:
    `_load_one_day(params, '2019-10.csv', date(2019,10,1), path, rpath, 0, buf)` //=>
    `1244245`
    '''

    try:
        with psycopg.connect(**connection_params) as connection:
            # Настройки сессии применяются один раз на соединение.
            connection.execute("SET TIME ZONE 'UTC'")
            connection.execute('SET synchronous_commit TO OFF')

            _acquire_day_lock(connection, source_file, batch_date)

            # Транзакция 1: фиксация старта (переживает сбой COPY).
            _journal_running(connection, source_file, batch_date)

            connection.commit()

            # Транзакция 2: COPY атомарен целиком. Очистка прежнего
            # batch выполнена один раз до пула воркеров (bulk).
            rows_loaded = 0

            if stage_file is not None and stage_file.exists():
                started = time.monotonic()

                with connection.cursor() as cursor:
                    rows_loaded = _copy_stream(
                        cursor,
                        stage_file,
                        buffer_size,
                    )

                rate = rows_loaded / max(time.monotonic() - started, 1e-9)

                log.info(
                    'Day COPY completed: batch_date=%s, rows=%s, rate=%.0f rows/s',
                    batch_date,
                    f'{rows_loaded:,}',
                    rate,
                )
            else:
                # INFO: в режиме month календарно пустой день — норма;
                # аномалия нулевого результата целиком фиксируется в _report.
                log.info(
                    'No staged rows for day, skipping COPY: batch_date=%s',
                    batch_date,
                )

            connection.commit()

            # Транзакция 3: финализация журнала.
            _journal_finished(
                connection,
                source_file,
                batch_date,
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
            )

            connection.commit()

        # Блокировка освобождена закрытием соединения. Файлы удаляются
        # только после успешного завершения всех транзакций.
        if stage_file is not None:
            stage_file.unlink(missing_ok=True)

        if reject_file is not None:
            reject_file.unlink(missing_ok=True)

    except Exception as exc:
        log.exception(
            'Day load failed: batch_date=%s',
            batch_date,
        )

        # Отметка об ошибке живёт в отдельном соединении и отдельной
        # транзакции: она обязана закоммититься независимо от неудачи
        # загрузки (исходное соединение aborted и закрыто контекстом).
        try:
            with psycopg.connect(**connection_params) as connection:
                _journal_finished(
                    connection,
                    source_file,
                    batch_date,
                    rows_loaded=0,
                    rows_rejected=0,
                    error=str(exc),
                )

        except Exception:
            log.exception(
                'Journal failure record failed: batch_date=%s',
                batch_date,
            )

        raise

    return rows_loaded


def _load_days_parallel(
    connection_params: dict,
    source_file: str,
    stats: ScanStats,
    target_days: set[date],
    copy_workers: int,
    buffer_size: int,
    skip_dates: set[date]
) -> dict[date, int]:
    '''
    Параллельная загрузка дневных batch.

    Каждый воркер владеет собственным соединением; дни назначаются
    взаимоисключающе, конфликтов записи между потоками нет. Потоки
    достаточны — работа преимущественно ввод-вывод.

    Ошибка одного дня не отменяет успешно залитые остальные:
    состояние каждого дня видно в журнале, возобновление точечное.

    :param dict connection_params: Параметры подключения.
    :param str source_file: Имя исходного файла.
    :param ScanStats stats: Итоги сканирования (пути файлов, карантин).
    :param set[date] target_days: Целевые дни.
    :param int copy_workers: Число параллельных воркеров.
    :param int buffer_size: Размер буфера COPY в байтах.
    :param set[date] skip_dates: Дни, пропускаемые по журналу (resume).
    :return dict[date, int]: Дата -> число загруженных строк.
    :raises RuntimeError: Если хотя бы один день завершился ошибкой.

    :example:
    `_load_days_parallel(params, '2019-10.csv', stats, days, 4, 32 << 20)` //=>
    `{date(2019,10,1): 1244245}`
    '''

    def worker(batch_date: date) -> int:
        day_key = batch_date.isoformat()

        return _load_one_day(
            connection_params=connection_params,
            source_file=source_file,
            batch_date=batch_date,
            stage_file=stats.day_files.get(day_key),
            reject_file=stats.reject_files.get(day_key),
            rows_rejected=stats.day_rejected.get(day_key, 0),
            buffer_size=buffer_size,
        )

    results: dict[date, int] = {}
    failures: dict[date, BaseException] = {}

    pending_days = sorted(target_days - skip_dates)

    skipped = sorted(target_days & skip_dates)

    if skipped:
        log.info(
            'Resume: skipped loaded days=%s',
            len(skipped),
        )

    with ThreadPoolExecutor(max_workers=copy_workers) as pool:
        futures = {
            batch_date: pool.submit(worker, batch_date)
            for batch_date in pending_days
        }

        for batch_date, future in futures.items():
            try:
                results[batch_date] = future.result()

            except Exception as exc:
                failures[batch_date] = exc

    if failures:
        raise RuntimeError(
            'Загрузка дней завершилась с ошибками: '
            + ', '.join(
                f'{day.isoformat()}={error}'
                for day, error in failures.items()
            ),
        )

    return results

def _restore_autovacuum(
    connection: psycopg.Connection,
    target_days: set[date],
) -> None:
    '''
    Возвращает autovacuum на партициях целевого периода после загрузки.

    Партиции создаются с autovacuum_enabled=false (конкуренция уборки
    с массовым COPY). Проверка существования не требуется: партиции
    гарантированно созданы в _ensure_partitions этого же запуска.

    :param psycopg.Connection connection: Соединение с PostgreSQL.
    :param set[date] target_days: Целевые дни.

    :example:
    `_restore_autovacuum(connection, target_days)` //=>
    `autovacuum включён на затронутых партициях`
    '''

    for year, month in _adjacent_months(target_days):
        connection.execute(
            sql.SQL('ALTER TABLE ods.{} SET (autovacuum_enabled = true)')
            .format(sql.Identifier(_partition_name(date(year, month, 1)))),
        )

    connection.commit()

# =========================================================================
# CLI и orchestration.
# =========================================================================

def _parse_args() -> argparse.Namespace:
    '''
    Разбирает аргументы командной строки.

    - режим `day` точечно загружает/перезагружает один день (перезагрузка,
    отладка, бенчмарк); 
    - режим `month` проводит полный бэкфилл месяца
    одним проходом по файлу и предпочтителен для массовой заливки.

    :return argparse.Namespace: Аргументы CLI.

    :example:
    `_parse_args()` //=>
    `Namespace(source_file=..., batch_date=..., load_mode='day')`
    '''

    parser = argparse.ArgumentParser(
        description='Загрузка событийного batch в PostgreSQL ODS.',
    )

    parser.add_argument(
        '--source-file',
        type=Path,
        required=True,
        help='Путь к исходному CSV-файлу.',
    )

    parser.add_argument(
        '--batch-date',
        type=date.fromisoformat,
        required=True,
        help=(
            'Опорная дата в формате YYYY-MM-DD: '
            'день batch (day) либо представитель месяца (month).'
        ),
    )

    parser.add_argument(
        '--load-mode',
        choices=('day', 'month'),
        default='day',
        help='Режим загрузки: один день или весь месяц одним проходом.',
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help=(
            'Пропустить дни, уже загруженные по журналу (status=loaded). '
            'Без флага существующие batch удаляются и загружаются заново. '
            'Перезагрузка месяца с нуля при resume требует предварительной '
            'чистки журнала: '
            "DELETE FROM ingestion.journal WHERE source_file='<имя файла>';"
        ),
    )

    return parser.parse_args()


def _report(
    source_file: str,
    stats: ScanStats,
    results: dict[date, int],
    elapsed: float,
    rows_loaded_base: int = 0,
) -> None:
    '''
    Сводка по запуску с контролем баланса строк.

    Баланс 1: `staged == loaded` — расхождение сигнализирует о внешнем
    вмешательстве в ODS между стадиями.
    Баланс 2: `scanned == staged + rejected + outside` — расхождение
    сигнализирует о потере строк внутри конвейера.
    Нулевой результат не является тихим успехом.

    :param str source_file: Имя исходного файла.
    :param ScanStats stats: Итоги сканирования.
    :param dict[date, int] results: Загружено строк по дням.
    :param float elapsed: Общая длительность запуска, секунд.

    :example:
    `_report('2019-10.csv', stats, results, 182.0)` //=>
    `строка Batch completed`
    '''

    rows_staged = sum(stats.day_rows.values())
    rows_loaded = rows_loaded_base + sum(results.values())
    rows_rejected = sum(stats.quarantine.values())

    rows_accounted = rows_staged + rows_rejected + stats.rows_outside_target

    if rows_accounted != stats.rows_total:
        log.warning(
            'Row accounting mismatch: scanned=%s, accounted=%s '
            '(staged=%s, rejected=%s, outside=%s)',
            f'{stats.rows_total:,}',
            f'{rows_accounted:,}',
            f'{rows_staged:,}',
            f'{rows_rejected:,}',
            f'{stats.rows_outside_target:,}',
        )

    if rows_loaded != rows_staged:
        log.warning(
            'Row balance mismatch: staged=%s, loaded=%s',
            f'{rows_staged:,}',
            f'{rows_loaded:,}',
        )

    if rows_loaded == 0:
        log.warning(
            'Zero rows loaded: source_file=%s '
            '(empty period or all rows quarantined)',
            source_file,
        )

    log.info(
        'Batch completed: rows_scanned=%s, rows_loaded=%s, rows_rejected=%s, '
        'rows_outside_target=%s, elapsed=%.1fs, avg_rate=%.0f rows/s',
        f'{stats.rows_total:,}',
        f'{rows_loaded:,}',
        f'{rows_rejected:,}',
        f'{stats.rows_outside_target:,}',
        elapsed,
        rows_loaded / max(elapsed, 1e-9),
    )

    for reason, count in stats.quarantine.most_common():
        log.warning(
            'Quarantine: reason=%s, rows=%s',
            reason,
            f'{count:,}',
        )


def main() -> None:
    '''
    Запускает загрузку batch из командной строки.

    :example:
    `python -m src.ingestion.load_ods --source-file data/raw/2019-10.csv --batch-date 2019-10-01` //=>
    `Batch загружен в ods.events`
    '''

    configure_logging()

    args = _parse_args()

    settings = _settings()

    log.info(
        'Starting ODS ingestion: source_file=%s, batch_date=%s, load_mode=%s',
        args.source_file,
        args.batch_date,
        args.load_mode,
    )

    if not args.source_file.exists():
        raise FileNotFoundError(
            f'Исходный файл не найден: {args.source_file}',
        )

    # Точка отсчёта длительности всего запуска: включает SCAN, LOAD
    # и накладные расходы между стадиями.
    started = time.monotonic()

    # Единая техническая метка загрузки: все дни одного запуска (в
    # режиме month — весь месяц) получают одинаковое _ingested_at,
    # что делает batch атомарно идентифицируемым в данных.
    ingested_at_iso = datetime.now(timezone.utc).isoformat()

    # Определение целевого периода: один день (day) либо весь
    # календарный месяц (month). От него зависят объём сканирования,
    # набор staging-файлов и партиций.
    target_days = _target_days(args.load_mode, args.batch_date)

    # Стадия SCAN+SPILL: единственный проход по месячному файлу,
    # посуточные staging-файлы, карантин нарушений контракта и
    # статистика строк — вход для итогового баланса.
    stats = _scan_source(
        source_file=args.source_file,
        target_days=target_days,
        block_bytes=settings['read_block_bytes'],
        stage_dir=_stage_dir(settings['staging_dir'], args.source_file),
        ingested_at_iso=ingested_at_iso,
    )

    # Партиции создаются в главной нити до пула воркеров: гонка
    # CREATE TABLE между потоками исключена конструктивно,
    # состязание процессов обработано в _ensure_partitions
    # autovacuum возвращается самим загрузчиком: в рабочем режиме
    # init --stage indexes не выполняется, полагаться на него нельзя.
    with psycopg.connect(**settings['connection']) as connection:
        _restore_autovacuum(connection, target_days)
        _ensure_partitions(connection, target_days)

        # Дни loaded читаются в ОБЕИХ режимах до пула: после UPSERT
        # 'running' получить их из журнала уже невозможно.
        loaded_dates = _loaded_batch_dates(
            connection,
            args.source_file.name,
            target_days,
        )

        # Идемпотентность перезаливаемых дней: COPY коммитится
        # отдельной транзакцией до финализации журнала, сбой между
        # ними оставляет строки при статусе running/failed; повторная
        # попытка без чистки аппендит дубликаты (2020-04-30 ×3).
        # Поэтому чистятся ВСЕ перезаливаемые дни, а не только loaded.
        if args.resume:
            # Resume: loaded дни не трогаются; остальные чистятся
            # от остатков и заливаются.
            reload_dates = sorted(target_days - set(loaded_dates))

            if loaded_dates:
                log.info(
                    'Resume mode: skip loaded days=%s',
                    len(loaded_dates),
                )

        else:
            reload_dates = sorted(target_days)

        if reload_dates:
            log.info(
                'Removing existing batches: days=%s',
                len(reload_dates),
            )

            deleted = _delete_batches_bulk(
                connection,
                args.source_file.name,
                reload_dates,
            )

            log.info(
                'Existing batches removed: days=%s, rows_deleted=%s',
                len(reload_dates),
                f'{deleted:,}',
            )

    # Стадия LOAD: параллельная заливка дней; у каждого воркера
    # собственное соединение, транзакции и запись в журнал, ошибки
    # изолируются по дням (успешные дни не откатываются).
    results = _load_days_parallel(
        connection_params=settings['connection'],
        source_file=args.source_file.name,
        stats=stats,
        target_days=target_days,
        skip_dates=set(loaded_dates) if args.resume else set(),
        copy_workers=settings['copy_workers'],
        buffer_size=settings['copy_buffer_bytes'],
    )

    # Строки пропущенных дней уже в ODS: входят в итоговый баланс,
    # хотя в этом запуске не заливались.
    rows_preloaded = sum(
        stats.day_rows.get(day.isoformat(), 0)
        for day in (target_days - set(results))
    )

    # Сводка по запуску: балансы строк (scan/staged/loaded), карантин
    # по причинам, средняя скорость — контроль целостности прогона.
    _report(
        source_file=args.source_file.name,
        stats=stats,
        results=results,
        rows_loaded_base=rows_preloaded,
        elapsed=time.monotonic() - started,
    )


if __name__ == '__main__':
    main()