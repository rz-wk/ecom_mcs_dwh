'''
Загрузка конфигурации проекта.

Модуль объединяет общую и локальную конфигурацию и предоставляет единый интерфейс доступа к настройкам.

Для вложенных параметров используется dot-path.

:example:
`_conf.get('logging.level', 'INFO')` //=>
`'INFO'`

`_conf.get('databases.postgres_ods.host')` //=>
`'localhost'`
'''

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


class Config(dict):
    '''
    Конфигурация с доступом ко вложенным значениям через dot-path.

    :example:
    `config = Config({'database': {'host': 'localhost'}})`
    `config.get('database.host')` //=>
    `'localhost'`
    '''

    def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        '''
        Возвращает значение конфигурации по dot-path.

        :param str key: Путь к параметру через точку.
        :param Any default: Значение, возвращаемое при отсутствии параметра.
        :return Any: Значение параметра или default.

        :example:
        `config.get('database.host')` //=>
        `'localhost'`

        `config.get('database.port', 5432)` //=>
        `5432`
        '''

        value: Any = self

        for part in key.split('.'):
            if not isinstance(value, Mapping) or part not in value:
                return default

            value = value[part]

        return value


def _merge_dicts(
    base: Mapping[str, Any],
    override: Mapping[str, Any],
) -> dict[str, Any]:
    '''
    Рекурсивно объединяет словари конфигурации.

    Значения локальной конфигурации имеют приоритет над общими.
    Вложенные словари объединяются, а не заменяются целиком.

    :param Mapping base: Базовая конфигурация.
    :param Mapping override: Конфигурация с приоритетными значениями.
    :return dict: Объединенная конфигурация.

    :example:
    `_merge_dicts({'db': {'host': 'localhost'}}, {'db': {'port': 5432}})` //=>
    `{'db': {'host': 'localhost', 'port': 5432}}`
    '''

    result = deepcopy(dict(base))

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], Mapping)
            and isinstance(value, Mapping)
        ):
            result[key] = _merge_dicts(
                result[key],
                value,
            )
        else:
            result[key] = deepcopy(value)

    return result


def _build_database_urls(
    config: dict,
) -> dict:
    '''
    Формирует URL подключений к базам данных для SQLAlchemy/DBAPI: `driver://user:password@host:port/database`

    :param dict config: Объединенный конфиг проекта.
    :return dict: Конфиг с добавленными URL подключений.

    :example:
    `_build_database_urls(config)` //=>
    `config + databases.postgres_ods.url`
    '''

    databases = config.get('databases', {})

    required = (
        'driver',
        'user',
        'password',
        'host',
        'port',
        'database',
    )

    for name, database in databases.items():
        missing = [
            key for key in required
            if key not in database
        ]

        if missing:
            continue

        database['url'] = (
            f"{database['driver']}://"
            f"{database['user']}:{database['password']}@"
            f"{database['host']}:{database['port']}/"
            f"{database['database']}"
        )

    return config


def load_config() -> Config:
    '''
    Загружает и объединяет общую и локальную конфигурацию.

    Локальный файл импортируется отдельно, credentials
    не находятся в общем конфигурационном файле.

    :return Config: Объединенная конфигурация.

    :example:
    `load_config()` //=>
    `_conf с общими и локальными настройками`
    '''

    from config.config import _conf as base_conf
    from config.config_local import _conf as local_conf

    config = _merge_dicts(
        base=base_conf,
        override=local_conf,
    )

    return Config(
        _build_database_urls(config),
    )

_conf = load_config()