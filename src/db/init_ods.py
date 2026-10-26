'''
Инициализация PostgreSQL ODS.

Модуль последовательно выполняет SQL-скрипты из `sql/ods/` стадиями:

    base (по умолчанию):
        1. 01_schema.sql
        2. 02_tables.sql

    indexes:
        1. 03_indexes.sql
        2. 04_analyze.sql
        3. 05_maintenance.sql

Стадии необходимы потому, что индексы на партиционированной таблице
создаются как partitioned index и автоматически наследуются каждой
новой партицией. Единоразовый запуск всех скриптов до бэкфилла
заставит загрузку млн строк идти с поддержкой индексов.

Порядок вызова:

    1. `src.db.init_ods` — схема и таблицы (до бэкфилла);
    2. бэкфилл через `src.ingestion.load_ods` — загрузка без индексов;
    3. `src.db.init_ods --stage indexes` — индексы на готовых данных
       и статистика планировщика.


Стадии идемпотентны: 
    - `CREATE ... IF NOT EXISTS` и повторный `ANALYZE`
    безвредны;
    - Скрипты одной стадии выполняются в рамках одной транзакции — при ошибке изменения не фиксируются.

Модуль не загружает данные. Загрузка выполняется через
`src.ingestion.load_ods`.

Параметры подключения и логирования получает из конфигурации проекта.

:example:
`python -m src.db.init_ods` //=>
`2026-08-13 01:30:00 | INFO | __main__ | SQL-скрипт выполнен: 01_schema.sql`

`python -m src.db.init_ods --stage indexes` //=>
`2026-08-13 02:00:00 | INFO | __main__ | SQL-скрипт выполнен: 03_indexes.sql`
'''

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from sqlalchemy import create_engine, text

from config import _conf
from src.utils.logging import configure_logging
from src.utils.sql import validate_sql_file


log = logging.getLogger(__name__)


# Состав этапа: список SQL-скриптов в порядке выполнения.
_SQL_FILES_BY_STAGE = {
    'base': (
        '01_schema.sql',
        '02_tables.sql',
    ),
    'indexes': (
        '03_indexes.sql',
        '04_analyze.sql',
        '05_maintenance.sql',
    ),
}


def _parse_args() -> argparse.Namespace:
    '''
    Разбирает аргументы командной строки.

    :return argparse.Namespace: Аргументы CLI.

    :example:
    `_parse_args()` //=>
    `Namespace(stage='base')`
    '''

    parser = argparse.ArgumentParser(
        description='Инициализация PostgreSQL ODS.',
    )

    parser.add_argument(
        '--stage',
        choices=tuple(_SQL_FILES_BY_STAGE),
        default='base',
        help=(
            'Стадия инициализации: base — схема и таблицы до бэкфилла; '
            'indexes — индексы и статистика после бэкфилла.'
        ),
    )

    return parser.parse_args()


def _get_sql_dir() -> Path:
    '''
    Возвращает путь к каталогу SQL-скриптов ODS.

    :return Path: Абсолютный путь к `sql/ods`.

    :example:
    `_get_sql_dir()` //=>
    `Path('/project/sql/ods')`
    '''

    project_root = Path(__file__).resolve().parents[2]

    return project_root / 'sql' / 'ods'


def _execute_sql_scripts(
    engine,
    sql_dir: Path,
    stage: str,
) -> None:
    '''
    Последовательно выполняет SQL-скрипты стадии в одной транзакции.

    Остановка по невалидному скрипту прерывает транзакцию.

    :param Engine engine: SQLAlchemy engine PostgreSQL.
    :param Path sql_dir: Каталог с SQL-скриптами.
    :param str stage: Имя стадии инициализации.

    :raises RuntimeError: Если хотя бы один скрипт невалиден.

    :example:
    `_execute_sql_scripts(engine, Path('sql/ods'), 'base')` //=>
    `Все SQL-скрипты стадии выполнены`
    '''

    log.info(
        'Начало инициализации ODS: stage=%s, sql_dir=%s',
        stage,
        sql_dir,
    )

    with engine.begin() as connection:
        for filename in _SQL_FILES_BY_STAGE[stage]:
            path = sql_dir / filename

            result = validate_sql_file(
                path=path,
                dialect='postgres',
            )

            if not result.is_valid:
                # Ошибка валидации должна останавливать процесс явно
                raise RuntimeError(
                    f'SQL-скрипт невалиден, инициализация остановлена: {filename}',
                )

            if result.is_empty:
                continue

            connection.execute(
                text(result.query),
            )

            log.info(
                'SQL-скрипт выполнен: %s',
                filename,
            )

    log.info(
        'Инициализация ODS завершена: stage=%s',
        stage,
    )


def main() -> None:
    '''
    Инициализирует PostgreSQL ODS из SQL-скриптов проекта.

    Любая ошибка журналируется с трассировкой и пробрасывается дальше:
    процесс должен завершаться ненулевым кодом.

    :example:
    `python -m src.db.init_ods` //=>
    `ODS инициализирован (stage=base)`
    '''

    configure_logging()

    args = _parse_args()

    sql_dir = _get_sql_dir()

    database_url = _conf.get(
        'databases.postgres_ods.url',
    )

    log.info(
        'Запуск инициализации PostgreSQL ODS',
    )

    engine = create_engine(
        database_url,
    )

    try:
        _execute_sql_scripts(
            engine=engine,
            sql_dir=sql_dir,
            stage=args.stage,
        )

    except Exception:
        # exception, а не error: сохраняет полную трассировку исходного сбоя.
        log.exception(
            'Ошибка инициализации PostgreSQL ODS',
        )

        raise

    finally:
        engine.dispose()


if __name__ == '__main__':
    main()