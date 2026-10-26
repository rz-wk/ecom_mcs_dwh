'''
Очистка и дедупликация событий ODS с загрузкой в ClickHouse Core.

Модуль:
    - читает дневной batch из PostgreSQL ODS через JDBC
      (с контролем полноты чтения);
    - выполняет проверки качества (quarantine);
    - удаляет полные дубликаты исходных событий;
    - формирует детерминированный event_id;
    - проверяет уникальность event_id;
    - загружает результат в ClickHouse Core;
    - выполняет контрольные проверки после загрузки.

Дедупликация выполняется только для полностью идентичных
исходных событий.

:example:
`python -m src.pyspark.deduplicate_events --batch-date 2019-10-01 --source-file 2019-10.csv`
'''

from __future__ import annotations

import argparse
import logging
import sys
import os
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Iterator
from decimal import Decimal

import pandas as pd
from clickhouse_driver import Client
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

import warnings

warnings.filterwarnings(
    'ignore',
    message='PySpark does not yet fully support pandas',
    category=FutureWarning,
)

from config import _conf
from src.utils.logging import configure_logging


log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Локальный PostgreSQL JDBC jar вместо spark.jars.packages:
# наличие packages запускает Ivy-резолюцию Maven при каждом 
# старте (вывод Ivy в stdout + секунды на резолюцию). 
# Jar обновляется вручную: jars/postgresql-42.7.7.jar в корне проекта.
_JDBC_JAR_PATH = _PROJECT_ROOT / 'jars' / 'postgresql-42.7.7.jar'

_ODS_TABLE = 'ods.events'
_CORE_TABLE = 'ecom_mcs_dwh_core.fact_events'

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

# TIMESTAMPTZ-колонки ODS: на чтении выгружаются как EPOCH,
# инстанты восстанавливаются в Spark
# (см. _get_ods_select_items, _rebuild_instants).
_TIMESTAMPTZ_COLUMNS = (
    'event_time',
)

# TimestampType-колонка -> epoch-колонка. Источник истины:
# EXTRACT(EPOCH ...) вычисляет PostgreSQL из настоящего timestamptz,
# значение не проходит через таймзоны JDBC/Spark.
_EPOCH_COLUMNS = {
    'event_time': 'event_time_epoch',
    '_ingested_at': '_ingested_at_epoch',
}

# Spark TimestampType-колонки, передаваемые в ClickHouse DateTime64:
# read-side колонки + служебные timestamp-колонки Core.
# На вставке tz-aware значения приводятся к naive UTC
# (см. _prepare_insert_rows).
_TIMESTAMP_COLUMNS = _TIMESTAMPTZ_COLUMNS + ('_ingested_at',)

# Целевые нативные типы колонок для clickhouse-driver
# (см. _prepare_insert_rows).
_STRING_COLUMNS = frozenset((
    'event_type',
    'category_code',
    'brand',
    'user_session',
    '_source_file',
))

_INTEGER_COLUMNS = frozenset((
    'event_id',
    'product_id',
    'category_id',
    'user_id',
))

_DATE_COLUMNS = frozenset((
    'event_date',
    '_batch_date',
))

_DECIMAL_COLUMNS = frozenset((
    'price',
))

# Валидные имена JDBC-опций партиционирования: Spark принимает
# только точные имена, незнакомые молча игнорирует.
_JDBC_PARTITION_OPTIONS = frozenset((
    'partitionColumn',
    'lowerBound',
    'upperBound',
    'numPartitions',
))

_DEDUP_COLUMNS = _SOURCE_COLUMNS

_CORE_COLUMNS = (
    'event_id',
    'event_time',
    'event_type',
    'product_id',
    'category_id',
    'category_code',
    'brand',
    'price',
    'user_id',
    'user_session',
    'event_date',
    '_source_file',
    '_batch_date',
    '_ingested_at',
)

_VALID_EVENT_TYPES = (
    'view',
    'cart',
    'remove_from_cart',
    'purchase',
)

_WRITE_STAT_SCHEMA = StructType([
    StructField('rows_written', LongType(), nullable=False),
])

_INSERT_SQL = (
    f'INSERT INTO {_CORE_TABLE} '
    f"({', '.join(_CORE_COLUMNS)}) VALUES"
)


def _parse_args() -> argparse.Namespace:
    '''
    Разбирает аргументы командной строки.

    :return argparse.Namespace: Аргументы CLI.

    :example:
    `_parse_args()` //=>
    `Namespace(batch_date=..., source_file=...)`
    '''

    parser = argparse.ArgumentParser(
        description='Очистка и дедупликация ODS batch.',
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
        help='Имя исходного CSV-файла.',
    )

    return parser.parse_args()


def _create_spark() -> SparkSession:
    '''
    Создает SparkSession с PostgreSQL JDBC driver.

    Опциональные настройки Spark применяются только при наличии
    соответствующих параметров в конфигурации.

    :return SparkSession: SparkSession проекта.

    :example:
    `_create_spark()` //=>
    `SparkSession`
    '''
    # SPARK_CONF_DIR обязан быть установлен до старта JVM: 
    # JVM наследует окружение Python-процесса в момент getOrCreate(),
    # log4j2 загружается при инициализации JVM. Из этого каталога
    # берется spark-conf/log4j2.properties (root logger = error):
    # без него Spark печатает дефолтный профиль, подсказку про
    # setLogLevel и WARN NativeCodeLoader в stdout.
    # setdefault — не перетирает значение, заданное снаружи.
    os.environ.setdefault(
        'SPARK_CONF_DIR',
        str(_PROJECT_ROOT / 'spark-conf'),
    )

    # spark.jars.packages не используется (см. _JDBC_JAR_PATH):
    # jar подключается локально. Проверка до билдера
    if not _JDBC_JAR_PATH.exists():
        raise FileNotFoundError(
            f'PostgreSQL JDBC jar не найден: {_JDBC_JAR_PATH}',
        )

    builder = (
        SparkSession.builder
        .appName('ecom-mcs-dwh-deduplicate')
    )

    master = _conf.get('spark.master')

    if master is not None:
        builder = builder.master(master)

    optional_configs = {
        'spark.driver.cores': _conf.get('spark.driver_cores'),
        'spark.sql.shuffle.partitions': _conf.get(
            'spark.shuffle_partitions',
        ),
        # Локальный jar вместо spark.jars.packages -
        # без Ivy-резолюции
        'spark.jars': str(_JDBC_JAR_PATH),
        # 'spark.jars.packages': _conf.get(
        #     'spark.jdbc.package',
        # ),
        'spark.sql.session.timeZone': _conf.get(
            'spark.session_timezone',
            'UTC',
        ),
        # PYTHONWARNINGS читается при старте Python-воркера:
        # каждый воркер mapInPandas печатает FutureWarning о
        # pandas >= 3.0 — гасится на уровне воркеров. 
        # Драйвер подавляется отдельно в шапке модуля: 
        # warnings.filterwarnings (процесс драйвера уже запущен до билдера).
        'spark.executorEnv.PYTHONWARNINGS': _conf.get(
            'spark.executor_pythonwarnings',
            'ignore::FutureWarning',
        ),
    }

    for key, value in optional_configs.items():
        if value is not None:
            builder = builder.config(key, value)

    return builder.getOrCreate()


def _get_postgres_properties() -> dict[str, str]:
    '''
    Возвращает параметры PostgreSQL JDBC.

    :return dict[str, str]: JDBC-параметры подключения.

    :example:
    `_get_postgres_properties()` //=>
    `{'user': '...', 'password': '...', 'driver': 'org.postgresql.Driver'}`
    '''

    database = _conf.get('databases.postgres_ods')

    return {
        'user': database['user'],
        'password': database['password'],
        'driver': _conf.get(
            'spark.jdbc.driver',
            'org.postgresql.Driver',
        ),
        'fetchsize': str(
            _conf.get(
                'spark.jdbc.fetchsize',
                10_000,
            ),
        ),
    }


def _get_postgres_jdbc_url() -> str:
    '''
    Формирует JDBC URL PostgreSQL без учетных данных.

    :return str: JDBC URL.
    '''

    database = _conf.get('databases.postgres_ods')

    return (
        f"jdbc:postgresql://"
        f"{database['host']}:"
        f"{database['port']}/"
        f"{database['database']}"
    )


def _read_jdbc(
    spark: SparkSession,
    properties: dict[str, str],
    dbtable: str,
    **partition_options: int | str,
) -> DataFrame:
    '''
    Читает JDBC-запрос; опционально с партиционированием.

    Имена опций партиционирования валидируются явно: Spark принимает
    только точные имена и молча игнорирует незнакомые, что отключает
    партиционирование без каких-либо сообщений.

    :param SparkSession spark: SparkSession.
    :param dict[str, str] properties: JDBC-параметры.
    :param str dbtable: Подзапрос чтения.
    :param partition_options: Опции JDBC-партиционирования.
    :return DataFrame: Данные запроса.
    '''

    for key in partition_options:
        if key not in _JDBC_PARTITION_OPTIONS:
            raise ValueError(
                f'Неизвестная JDBC-опция: {key}. '
                f'Допустимые: {sorted(_JDBC_PARTITION_OPTIONS)}',
            )

    reader = (
        spark.read
        .format('jdbc')
        .option(
            'url',
            _get_postgres_jdbc_url(),
        )
        .option('dbtable', dbtable)
        .options(**properties)
    )

    for key, value in partition_options.items():
        reader = reader.option(key, str(value))

    return reader.load()


def _ods_batch_filter(
    batch_date: date,
    source_file: str,
) -> str:
    '''
    Возвращает SQL-условие фильтрации ODS по ключу batch.

    Ключ (source_file, batch_date) единообразен с ключом удаления
    в Core и ключом карантина.

    :param date batch_date: Дата batch.
    :param str source_file: Имя исходного файла.
    :return str: SQL-условие для WHERE.
    '''

    source_file = source_file.replace("'", "''")

    return (
        f"_batch_date = '{batch_date.isoformat()}' "
        f"AND _source_file = '{source_file}'"
    )


def _get_ods_select_items() -> list[str]:
    '''
    Формирует SELECT-выражения чтения ODS.

    Таймзоны:
        TIMESTAMPTZ-колонки читаются как EPOCH, а не как timestamp.
        Вариант `AT TIME ZONE 'UTC'` не работает: колонка уходит
        в JDBC как naive-значение, pgjdbc интерпретирует его
        в таймзоне по умолчанию JVM, и Spark получает инстант,
        сдвинутый на смещение JVM (наблюдалось -5ч). Старый RDD
        write-путь рендерил инстанты обратно в той же таймзоне —
        сдвиги взаимно гасились; Arrow-путь отдает значение как есть,
        и сдвиг чтения становится видимым.

        EXTRACT(EPOCH ...) вычисляется сервером PostgreSQL из
        настоящего timestamptz. Инстанты восстанавливаются
        через timestamp_seconds (_rebuild_instants), event_date
        строится из epoch (_prepare_core). Результат не зависит
        ни от таймзоны JVM, ни от spark.sql.session.timeZone.

    :return list[str]: Выражения для SELECT.
    '''

    items = [
        column
        for column in _SOURCE_COLUMNS
        if column not in _TIMESTAMPTZ_COLUMNS
    ]

    items += [
        f'EXTRACT(EPOCH FROM {column}) AS {column}_epoch'
        for column in _TIMESTAMPTZ_COLUMNS
    ]

    items += [
        '_source_file',
        '_batch_date',
        'EXTRACT(EPOCH FROM _ingested_at) AS _ingested_at_epoch',
    ]

    return items


def _ods_subquery(
    batch_date: date,
    source_file: str,
    extra_where: str | None = None,
) -> str:
    '''
    Формирует подзапрос чтения batch из ODS.

    :param date batch_date: Дата batch.
    :param str source_file: Имя исходного файла.
    :param str | None extra_where: Дополнительное условие WHERE.
    :return str: Подзапрос для JDBC dbtable.
    '''

    where = _ods_batch_filter(batch_date, source_file)

    if extra_where is not None:
        where = f'{where} AND {extra_where}'

    return (
        '('
        f'SELECT {", ".join(_get_ods_select_items())} '
        f'FROM {_ODS_TABLE} '
        f'WHERE {where}'
        ') AS events_batch'
    )


def _read_ods_bounds(
    spark: SparkSession,
    properties: dict[str, str],
    batch_date: date,
    source_file: str,
) -> tuple[int | None, int | None, int, int]:
    '''
    Определяет параметры чтения batch: границы user_id для
    партиционирования, эталонное количество строк и количество строк
    с NULL user_id.

    :param SparkSession spark: SparkSession.
    :param dict[str, str] properties: JDBC-параметры.
    :param date batch_date: Дата batch.
    :param str source_file: Имя исходного файла.
    :return tuple[int | None, int | None, int, int]:
        Минимальный user_id (NULL, если все user_id в batch NULL),
        максимальный user_id, количество строк batch,
        количество строк с NULL user_id.

    :example:
    `_read_ods_bounds(spark, properties, date(2019, 10, 1), '2019-10.csv')` //=>
    `(10000000, 580000000, 1244245, 0)`
    '''

    query = (
        '('
        'SELECT'
        ' MIN(user_id) AS min_user_id,'
        ' MAX(user_id) AS max_user_id,'
        ' COUNT(*) AS total_rows,'
        ' COUNT(*) FILTER (WHERE user_id IS NULL) AS null_user_id_rows'
        f' FROM {_ODS_TABLE}'
        f' WHERE {_ods_batch_filter(batch_date, source_file)}'
        ') AS batch_bounds'
    )

    bounds = _read_jdbc(
        spark=spark,
        properties=properties,
        dbtable=query,
    ).first()

    if bounds is None:
        raise RuntimeError(
            'Запрос параметров batch не вернул строк.',
        )

    source_total = int(bounds['total_rows'])

    if source_total == 0:
        raise ValueError(
            f'Batch не содержит событий: batch_date={batch_date}, '
            f'source_file={source_file}',
        )

    return (
        int(bounds['min_user_id'])
        if bounds['min_user_id'] is not None
        else None,
        int(bounds['max_user_id'])
        if bounds['max_user_id'] is not None
        else None,
        source_total,
        int(bounds['null_user_id_rows']),
    )


def _read_ods_batch(
    spark: SparkSession,
    batch_date: date,
    source_file: str,
) -> tuple[DataFrame, int]:
    '''
    Читает дневной batch из PostgreSQL ODS через JDBC.

    Основное чтение партиционируется по user_id. Строки с NULL user_id
    не попадают в партиционные предикаты, поэтому добираются отдельным
    непартиционным запросом — только при их наличии в batch
    (количество известно из запроса границ).

    :param SparkSession spark: SparkSession.
    :param date batch_date: Дата batch.
    :param str source_file: Имя исходного файла.
    :return tuple[DataFrame, int]: События batch (основное чтение +
        строки с NULL user_id) и эталонное количество строк в источнике.

    :example:
    `_read_ods_batch(spark, date(2019, 10, 1), '2019-10.csv')` //=>
    `(DataFrame событий ODS, 1244245)`
    '''

    properties = _get_postgres_properties()

    (
        min_user_id,
        max_user_id,
        source_total,
        null_user_id_rows,
    ) = _read_ods_bounds(
        spark=spark,
        properties=properties,
        batch_date=batch_date,
        source_file=source_file,
    )

    main_query = _ods_subquery(batch_date, source_file)

    if min_user_id is None:
        # Все user_id в batch NULL: партиционирование невозможно,
        # batch читается одним запросом и полностью уйдет в карантин.
        return _rebuild_instants(
            _read_jdbc(
                spark=spark,
                properties=properties,
                dbtable=main_query,
            ),
        ), source_total

    num_partitions = int(
        _conf.get(
            'spark.jdbc.num_partitions',
            8,
        ),
    )

    dataframe = _read_jdbc(
        spark=spark,
        properties=properties,
        dbtable=main_query,
        partitionColumn='user_id',
        lowerBound=min_user_id,
        upperBound=max_user_id,
        numPartitions=num_partitions,
    )

    if null_user_id_rows > 0:
        dataframe = dataframe.unionByName(
            _read_jdbc(
                spark=spark,
                properties=properties,
                dbtable=_ods_subquery(
                    batch_date,
                    source_file,
                    'user_id IS NULL',
                ),
            ),
        )

    return _rebuild_instants(dataframe), source_total

def _rebuild_instants(dataframe: DataFrame) -> DataFrame:
    '''
    Восстанавливает timestamp-колонки из epoch-значений.

    timestamp_seconds строит инстант напрямую из секунд эпохи,
    без участия таймзон. Достаточно точности double
    для миллисекунд DateTime64(3).
    '''

    return dataframe.withColumns(
        {
            column: F.timestamp_seconds(
                F.col(epoch_column).cast('double'),
            )
            for column, epoch_column in _EPOCH_COLUMNS.items()
        },
    )

def _normalize_events(
    dataframe: DataFrame,
) -> DataFrame:
    '''
    Нормализует поля событий перед проверками качества.

    Нормализация:
        - удаляет пробелы по краям строковых полей;
        - заменяет пустые строки на NULL;
        - приводит строковые поля к StringType.

    :param DataFrame dataframe: Исходный DataFrame.
    :return DataFrame: Нормализованный DataFrame.
    '''

    string_columns = (
        'event_type',
        'category_code',
        'brand',
    )

    for column in string_columns:
        dataframe = dataframe.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == '',
                F.lit(None),
            ).otherwise(
                F.trim(F.col(column)),
            ).cast(StringType()),
        )

    return dataframe


def _flag_quarantine(
    dataframe: DataFrame,
) -> DataFrame:
    '''
    Добавляет колонку quarantine_reason.

    Колонка содержит причину отбраковки первого сработавшего условия
    или NULL для корректных событий. Порядок условий в списке
    определяет приоритет причин.

    :param DataFrame dataframe: Нормализованный DataFrame.
    :return DataFrame: DataFrame с колонкой quarantine_reason.
    '''

    conditions = [
        (
            ~F.col('event_type').isin(*_VALID_EVENT_TYPES),
            'invalid_event_type',
        ),
        (
            F.col('event_type').isNull(),
            'null_event_type',
        ),
        (
            F.col('event_time').isNull(),
            'null_event_time',
        ),
        (
            F.col('product_id').isNull(),
            'null_product_id',
        ),
        (
            F.col('category_id').isNull(),
            'null_category_id',
        ),
        (
            F.col('price').isNull(),
            'null_price',
        ),
        (
            F.col('price') < 0,
            'negative_price',
        ),
        (
            F.col('user_id').isNull(),
            'null_user_id',
        ),
        (
            F.col('user_session').isNull(),
            'null_user_session',
        ),
    ]

    quarantine_reason = None

    for condition, reason in conditions:
        # Цепочка when: первое совпадение фиксирует причину.
        quarantine_reason = (
            F.when(condition, F.lit(reason))
            if quarantine_reason is None
            else quarantine_reason.when(condition, F.lit(reason))
        )

    return dataframe.withColumn(
        'quarantine_reason',
        quarantine_reason,
    )


def _write_quarantine(
    dataframe: DataFrame,
    rows_count: int,
    batch_date: date,
    source_file: str,
) -> int:
    '''
    Записывает некорректные события в quarantine dataset.

    Quarantine хранится в Parquet и разделяется по batch_date
    и source_file — синхронно с ключом удаления в Core.
    Для повторного запуска конкретный batch заменяется.

    :param DataFrame dataframe: Некорректные события.
    :param int rows_count: Количество некорректных событий
        (из единой агрегации Data Quality).
    :param date batch_date: Дата batch.
    :param str source_file: Исходный файл.
    :return int: Количество записанных записей.
    '''

    if rows_count == 0:
        log.info(
            'Quarantine empty: source_file=%s, batch_date=%s',
            source_file,
            batch_date,
        )
        return 0

    quarantine_root = Path(
        _conf.get(
            'paths.quarantine',
            'data/quarantine',
        ),
    )

    batch_path = (
        quarantine_root
        / f'batch_date={batch_date.isoformat()}'
        / f'source_file={source_file}'
    )

    (
        dataframe
        .write
        .mode('overwrite')
        .parquet(str(batch_path))
    )

    log.warning(
        'Quarantine written: source_file=%s, batch_date=%s, rows=%s',
        source_file,
        batch_date,
        f'{rows_count:,}',
    )

    return rows_count


def _deduplicate_events(
    dataframe: DataFrame,
    rows_before: int,
) -> tuple[DataFrame, int]:
    '''
    Удаляет полные дубликаты исходных событий.

    Дубликатом считается запись, полностью совпадающая
    по всем исходным полям события. Из группы дубликатов
    сохраняется запись с минимальным _ingested_at — результат
    детерминирован между повторными запусками batch.

    При равных _ingested_at (PostgreSQL now() возвращает одно время
    для всех строк транзакции) выбор записи внутри группы
    недетерминирован; на бизнес-поля и event_id это не влияет —
    различаются только служебные поля выжившей строки.

    Результат кэшируется: используется проверкой уникальности
    event_id и загрузкой в Core.

    :param DataFrame dataframe: Валидные события.
    :param int rows_before: Количество строк до дедупликации
        (из единой агрегации Data Quality).
    :return tuple[DataFrame, int]: DataFrame после дедупликации
        и количество строк после.

    :example:
    `_deduplicate_events(dataframe, 1244245)` //=>
    `(deduplicated_df, 1243715)`
    '''

    dedup_window = Window.partitionBy(*_DEDUP_COLUMNS).orderBy(
        F.col('_ingested_at').asc(),
    )

    deduplicated = (
        dataframe
        .withColumn('_dedup_rank', F.row_number().over(dedup_window))
        .filter(F.col('_dedup_rank') == 1)
        .drop('_dedup_rank')
        .cache()
    )

    rows_after = deduplicated.count()

    log.info(
        'Deduplication completed: rows_before=%s, rows_after=%s, '
        'duplicates_removed=%s',
        f'{rows_before:,}',
        f'{rows_after:,}',
        f'{rows_before - rows_after:,}',
    )

    return (
        deduplicated,
        rows_after,
    )


def _add_event_id(
    dataframe: DataFrame,
) -> DataFrame:
    '''
    Формирует детерминированный event_id.

    Используется xxHash64 по полям, определяющим содержимое события.
    Результат ограничивается положительным диапазоном Int64,
    совместимым с ClickHouse.

    :param DataFrame dataframe: DataFrame после дедупликации.
    :return DataFrame: DataFrame с event_id.

    :example:
    `_add_event_id(dataframe)` //=>
    `DataFrame содержит уникальный event_id`
    '''

    hash_columns = [
        F.col(column)
        for column in (
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
    ]

    return dataframe.withColumn(
        'event_id',
        F.pmod(
            F.xxhash64(*hash_columns),
            F.lit(9_223_372_036_854_775_807),
        ).cast('long'),
    )


def _validate_event_ids(
    dataframe: DataFrame,
) -> None:
    '''
    Проверяет уникальность event_id.

    :param DataFrame dataframe: DataFrame событий.
    :raises ValueError: При обнаружении дублирующихся event_id.

    :example:
    `_validate_event_ids(dataframe)` //=>
    `Уникальность event_id подтверждена`
    '''

    duplicate = (
        dataframe
        .groupBy('event_id')
        .count()
        .filter(F.col('count') > 1)
        .first()
    )

    if duplicate is not None:
        raise ValueError(
            'Обнаружены дубли event_id после дедупликации.',
        )

    log.info(
        'Event ID uniqueness check passed',
    )


def _prepare_core(
    dataframe: DataFrame,
) -> DataFrame:
    '''
    Приводит DataFrame к контракту ClickHouse Core.

    event_date вычисляется календарной арифметикой по epoch-значению
    event_time, полученному на этапе чтения ODS. В отличие от
    to_date(TimestampType), рендерящего дату в session timezone,
    результат не зависит от настроек таймзоны Spark.

    :param DataFrame dataframe: DataFrame после дедупликации.
    :return DataFrame: DataFrame для загрузки в Core.

    :example:
    `_prepare_core(dataframe)` //=>
    `DataFrame соответствует fact_events`
    '''

    return (
        dataframe
        .withColumn(
            'product_id',
            F.col('product_id').cast('long'),
        )
        .withColumn(
            'category_id',
            F.col('category_id').cast('long'),
        )
        .withColumn(
            'price',
            F.col('price').cast(
                DecimalType(12, 2),
            ),
        )
        .withColumn(
            'user_id',
            F.col('user_id').cast('long'),
        )
        .withColumn(
            'event_date',
            F.expr(
                "date_add(DATE'1970-01-01', "
                'CAST(FLOOR(event_time_epoch / 86400) AS INT))',
            ),
        )
        .select(*_CORE_COLUMNS)
    )


def _get_clickhouse_client() -> Client:
    '''
    Создает ClickHouse client через native TCP protocol.

    :return Client: ClickHouse client.

    :example:
    `_get_clickhouse_client()` //=>
    `Client(host='localhost', port=9005)`
    '''

    database = _conf.get('databases.clickhouse_core')

    return Client(
        host=database['host'],
        port=database['port'],
        user=database['user'],
        password=database['password'],
        database=database['database'],
    )


def _delete_core_batch(
    source_file: str,
    batch_date: date,
) -> None:
    '''
    Удаляет существующий batch из ClickHouse Core.

    Удаление выполняется синхронно, чтобы последующая загрузка
    формировала единственную актуальную версию batch.
    Неатомарность пары delete -> write закрывается идемпотентным
    повторным запуском batch.

    :param str source_file: Имя исходного файла.
    :param date batch_date: Дата batch.

    :example:
    `_delete_core_batch('2019-10.csv', date(2019, 10, 1))` //=>
    `Существующий batch удален`
    '''

    client = _get_clickhouse_client()

    try:
        client.execute(
            'SET mutations_sync = 1',
        )

        client.execute(
            f'''
            ALTER TABLE {_CORE_TABLE}
            DELETE WHERE
                _source_file = %(source_file)s
                AND _batch_date = %(batch_date)s
            ''',
            {
                'source_file': source_file,
                'batch_date': batch_date,
            },
        )

        log.info(
            'Existing Core batch removed: '
            'source_file=%s, batch_date=%s',
            source_file,
            batch_date,
        )

    finally:
        client.disconnect()


def _count_core_batch(
    source_file: str,
    batch_date: date,
) -> int:
    '''
    Возвращает количество строк batch в ClickHouse Core.

    Контрольная проверка после загрузки: сверка количества строк
    batch в Core с переданным количеством.

    Точность без FINAL: в рамках batch event_id уникален и входит
    в ORDER BY, поэтому ReplacingMergeTree не схлопывает строки batch
    до merge. Пересечения событий между разными batch (совпадающий
    ORDER BY-ключ) могут схлопываться при merge и приводить к ложному
    сбою проверки (in_core < written при корректной загрузке текущего
    batch) — принятое ограничение per-batch проверки уникальности.

    :param str source_file: Имя исходного файла.
    :param date batch_date: Дата batch.
    :return int: Количество строк batch в Core.

    :example:
    `_count_core_batch('2019-10.csv', date(2019, 10, 1))` //=>
    `1243715`
    '''

    client = _get_clickhouse_client()

    try:
        result = client.execute(
            f'''
            SELECT count()
            FROM {_CORE_TABLE}
            WHERE
                _source_file = %(source_file)s
                AND _batch_date = %(batch_date)s
            ''',
            {
                'source_file': source_file,
                'batch_date': batch_date,
            },
        )

        return int(result[0][0])

    finally:
        client.disconnect()


def _convert_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _convert_int(value: object) -> int:
    return int(value)


def _convert_date(value: object) -> date | None:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _convert_timestamp(value: object) -> datetime | None:
    if pd.isna(value):
        return None
    if value.tzinfo is not None:
        value = value.tz_convert('UTC').tz_localize(None)
    return value.to_pydatetime()


def _convert_decimal(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _build_column_converters() -> dict[str, Callable[[object], object]]:
    '''
    Возвращает конвертеры значений по колонкам Core.
    '''

    converters: dict[str, Callable[[object], object]] = {}

    for column in _STRING_COLUMNS:
        converters[column] = _convert_string

    for column in _INTEGER_COLUMNS:
        converters[column] = _convert_int

    for column in _DATE_COLUMNS:
        converters[column] = _convert_date

    for column in _TIMESTAMP_COLUMNS:
        converters[column] = _convert_timestamp

    for column in _DECIMAL_COLUMNS:
        converters[column] = _convert_decimal

    return converters


_COLUMN_CONVERTERS = _build_column_converters()


def _prepare_insert_rows(
    pdf: pd.DataFrame,
) -> Iterator[tuple]:
    '''
    Приводит pandas-батч к строкам для clickhouse-driver.

    Конвертация выполняется на уровне значений, без присвоений
    в колонки DataFrame: присвоение None в строковую колонку
    pandas >= 3.0 (string dtype по умолчанию) возвращает NaN,
    и значение снова становится float. Каждый скаляр принудительно
    приводится к нативному типу, ожидаемому драйвером
    (types_check=False типы не валидирует):
        - string -> str | None (NaN/None -> NULL);
        - integer -> int;
        - date -> datetime.date;
        - datetime -> datetime, naive UTC;
        - decimal -> Decimal.

    :param pd.DataFrame pdf: Батч из mapInPandas.
    :return Iterator[tuple]: Строки для INSERT.
    '''

    converters = [
        _COLUMN_CONVERTERS[column]
        for column in _CORE_COLUMNS
    ]

    for values in zip(*(pdf[column] for column in _CORE_COLUMNS)):
        yield tuple(
            converter(value)
            for converter, value in zip(converters, values)
        )


def _execute_insert(
    client: Client,
    buffer: list[tuple],
    pdf: pd.DataFrame | None,
) -> None:
    '''
    Выполняет один INSERT-батч.

    При ошибке логирует диагностику локализации колонки
    с некорректным значением: количество строк буфера, dtypes
    исходного pandas-батча, первую строку буфера.

    :param Client client: ClickHouse client.
    :param list[tuple] buffer: Строки INSERT.
    :param pd.DataFrame | None pdf: Последний обработанный батч.
    :raises Exception: Ошибка драйвера пробрасывается дальше.
    '''

    try:
        client.execute(_INSERT_SQL, buffer)

    except Exception:
        log.error(
            'ClickHouse insert failed: batch_rows=%s, pdf_dtypes=%s, '
            'first_row=%s',
            len(buffer),
            {
                column: str(dtype)
                for column, dtype in pdf.dtypes.items()
            }
            if pdf is not None
            else None,
            buffer[0] if buffer else None,
        )

        raise


def _write_partition(
    pdf_iterator: Iterator[pd.DataFrame],
) -> Iterator[pd.DataFrame]:
    '''
    Загружает одну Spark partition в ClickHouse.

    Вызывается через mapInPandas: данные передаются JVM -> Python
    колоночно через Arrow, без построчной сериализации Row.
    ClickHouse client создается внутри executor process,
    поэтому объект подключения не передается через Spark serialization.

    Функция возвращает однострочный DataFrame со счетчиком записанных
    строк — контракт mapInPandas требует DataFrame на выходе.

    :param Iterator[pd.DataFrame] pdf_iterator: Пакеты строк partition.
    :return Iterator[pd.DataFrame]: DataFrame со счетчиком строк.

    :example:
    `_write_partition(pdf_iterator)` //=>
    `DataFrame(rows_written=155000)`
    '''

    client = _get_clickhouse_client()

    batch_size = int(
        _conf.get(
            'spark.clickhouse.insert_batch_size',
            50_000,
        ),
    )

    rows_written = 0

    try:
        buffer: list[tuple] = []
        current_pdf: pd.DataFrame | None = None

        for pdf in pdf_iterator:
            current_pdf = pdf

            for row in _prepare_insert_rows(pdf):
                buffer.append(row)

                if len(buffer) >= batch_size:
                    _execute_insert(client, buffer, current_pdf)

                    rows_written += len(buffer)
                    buffer = []

        if buffer:
            _execute_insert(client, buffer, current_pdf)

            rows_written += len(buffer)

    finally:
        client.disconnect()

    yield pd.DataFrame({'rows_written': [rows_written]})


def _write_core_batch(
    dataframe: DataFrame,
) -> int:
    '''
    Загружает обработанный batch в ClickHouse Core.

    Коалесцирует партиции: после дедупликации их количество
    равно spark.sql.shuffle.partitions, что породило бы сотни
    мелких вставок. Возвращает количество переданных строк.

    :param DataFrame dataframe: DataFrame Core.
    :return int: Количество переданных в ClickHouse строк.

    :example:
    `_write_core_batch(dataframe)` //=>
    `1243715`
    '''

    write_partitions = int(
        _conf.get(
            'spark.clickhouse.write_partitions',
            8,
        ),
    )

    rows_written = int(
        (
            dataframe
            .coalesce(write_partitions)
            .mapInPandas(
                _write_partition,
                _WRITE_STAT_SCHEMA,
            )
            .agg(
                F.coalesce(
                    F.sum('rows_written'),
                    F.lit(0),
                ).alias('rows_written'),
            )
            .first()['rows_written']
        ),
    )

    log.info(
        'Core batch loaded: rows_written=%s',
        f'{rows_written:,}',
    )

    return rows_written


def process_batch(
    spark: SparkSession,
    batch_date: date,
    source_file: str,
) -> None:
    '''
    Выполняет полный pipeline обработки batch.

    :param SparkSession spark: SparkSession.
    :param date batch_date: Дата batch.
    :param str source_file: Имя исходного файла.
    :return None: Результат записывается в ClickHouse Core.

    :example:
    `process_batch(spark, date(2019, 10, 1), '2019-10.csv')` //=>
    `Batch обработан и загружен в Core`
    '''

    log.info(
        'Starting Core processing: source_file=%s, batch_date=%s',
        source_file,
        batch_date,
    )

    dataframe, source_total = _read_ods_batch(
        spark=spark,
        batch_date=batch_date,
        source_file=source_file,
    )

    dataframe = dataframe.cache()

    deduplicated = None

    try:
        checked = _flag_quarantine(
            _normalize_events(dataframe),
        )

        counts = (
            checked
            .agg(
                F.count(F.lit(1)).alias('total'),
                F.coalesce(
                    F.sum(
                        F.col('quarantine_reason')
                        .isNotNull()
                        .cast('long'),
                    ),
                    F.lit(0),
                ).alias('quarantine'),
            )
            .first()
        )

        total_count = int(counts['total'])
        quarantine_count = int(counts['quarantine'])
        valid_count = total_count - quarantine_count

        if total_count != source_total:
            raise RuntimeError(
                'Количество прочитанных строк не совпадает '
                'с источником (конкурентная запись в ODS '
                'или потеря при чтении): '
                f'source={source_total}, '
                f'read={total_count}',
            )

        _write_quarantine(
            dataframe=checked.filter(
                F.col('quarantine_reason').isNotNull(),
            ),
            rows_count=quarantine_count,
            batch_date=batch_date,
            source_file=source_file,
        )

        log.info(
            'Data Quality completed: valid=%s, quarantine=%s',
            f'{valid_count:,}',
            f'{quarantine_count:,}',
        )

        valid = (
            checked
            .filter(F.col('quarantine_reason').isNull())
            .drop('quarantine_reason')
        )

        deduplicated, rows_after = _deduplicate_events(
            valid,
            rows_before=valid_count,
        )

    finally:
        # Сырые данные больше не нужны: результат дедупликации
        # материализован (или pipeline прерван ошибкой).
        dataframe.unpersist()

    try:
        dedup_with_id = _add_event_id(
            deduplicated,
        )

        _validate_event_ids(
            dedup_with_id,
        )

        core = _prepare_core(
            dedup_with_id,
        )

        _delete_core_batch(
            source_file=source_file,
            batch_date=batch_date,
        )

        rows_written = _write_core_batch(
            core,
        )

        if rows_written != rows_after:
            raise RuntimeError(
                'Количество записанных строк не соответствует '
                'результату дедупликации: '
                f'expected={rows_after}, '
                f'written={rows_written}',
            )

        rows_in_core = _count_core_batch(
            source_file=source_file,
            batch_date=batch_date,
        )

        if rows_in_core != rows_written:
            raise RuntimeError(
                'Контрольная проверка после загрузки не пройдена: '
                f'expected={rows_written}, '
                f'in_core={rows_in_core}',
            )

        log.info(
            'Post-load check passed: rows_in_core=%s',
            f'{rows_in_core:,}',
        )

        log.info(
            'Core processing completed: '
            'source_file=%s, batch_date=%s, '
            'rows_before=%s, rows_after=%s, rows_written=%s',
            source_file,
            batch_date,
            f'{valid_count:,}',
            f'{rows_after:,}',
            f'{rows_written:,}',
        )

    finally:
        deduplicated.unpersist()


def main() -> None:
    '''
    Запускает обработку batch из командной строки.

    :example:
    `python -m src.pyspark.deduplicate_events --batch-date 2019-10-01 --source-file 2019-10.csv`
    '''

    configure_logging()

    args = _parse_args()
    spark = _create_spark()

    spark.sparkContext.setLogLevel('ERROR')

    try:
        process_batch(
            spark=spark,
            batch_date=args.batch_date,
            source_file=args.source_file,
        )

    except Exception as error:
        log.error(
            'Core processing failed: %s',
            error,
        )
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == '__main__':
    main()