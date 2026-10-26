'''
Инициализация ClickHouse Core.

Модуль:
    - создает базу данных Core;
    - последовательно выполняет SQL-скрипты из `sql/core/`;
    - ведет журнал выполнения.

Модуль не загружает данные.
Загрузка выполняется на этапе PySpark.

:example:
`python -m src.db.init_core` //=>
`ClickHouse Core успешно инициализирован`
'''

from __future__ import annotations

import logging
import sys
from pathlib import Path

from clickhouse_driver import Client

from config import _conf
from src.utils.logging import configure_logging
from src.utils.sql import validate_sql_file


log = logging.getLogger(__name__)


_SQL_FILES = (
    '01_database.sql',
    '02_fact_events.sql',
    '03_settings.sql',
)


def _get_sql_dir() -> Path:
    '''
    Возвращает путь к каталогу SQL-скриптов Core.

    :return Path: Абсолютный путь к `sql/core`.

    :example:
    `_get_sql_dir()` //=>
    `Path('/project/sql/core')`
    '''

    project_root = Path(__file__).resolve().parents[2]

    return project_root / 'sql' / 'core'


def _execute_sql_scripts(
    client: Client,
    sql_dir: Path,
) -> bool:
    '''
    Последовательно выполняет SQL-скрипты ClickHouse Core.

    :param Client client: Клиент ClickHouse.
    :param Path sql_dir: Каталог с SQL-скриптами.
    :return bool: True, если все скрипты выполнены.

    :example:
    `_execute_sql_scripts(client, Path('sql/core'))` //=>
    `True`
    '''

    log.info(
        'Начало инициализации ClickHouse Core: sql_dir=%s',
        sql_dir,
    )

    for filename in _SQL_FILES:
        path = sql_dir / filename

        result = validate_sql_file(
            path=path,
            dialect='clickhouse',
        )

        if not result.is_valid:
            log.error(
                'Инициализация Core остановлена: %s',
                filename,
            )
            return False

        if result.is_empty:
            continue

        # log.info(
        #     'Выполнение SQL-скрипта: %s',
        #     filename,
        # )

        client.execute(
            result.query,
        )

        log.info(
            'SQL-скрипт выполнен: %s',
            filename,
        )

    log.info(
        'Инициализация ClickHouse Core завершена',
    )

    return True


def main() -> None:
    '''
    Инициализирует ClickHouse Core.

    :example:
    `python -m src.db.init_core`
    `ClickHouse Core успешно инициализирован`
    '''

    configure_logging()

    database = _conf.get(
        'databases.clickhouse_core',
    )

    client = Client(
        host=database['host'],
        port=database['port'],
        user=database['user'],
        password=database['password'],
    )

    success = False

    try:
        success = _execute_sql_scripts(
            client=client,
            sql_dir=_get_sql_dir(),
        )

    except Exception as error:
        log.error(
            'Ошибка инициализации ClickHouse Core: %s',
            error,
        )

    finally:
        client.disconnect()

    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()