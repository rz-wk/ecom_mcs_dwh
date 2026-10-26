'''
Общие функции работы с SQL-файлами проекта.

Модуль:
    - читает SQL-файлы;
    - определяет наличие SQL-кода;
    - проверяет синтаксис через SQLGlot;
    - возвращает результат проверки.

:example:
`validate_sql_file(Path('sql/core/01_database.sql'), 'clickhouse')` //=>
`SQLValidationResult(query='...', is_valid=True, is_empty=False)`
'''

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot.errors import ParseError


log = logging.getLogger(__name__)


_SQL_COMMENT_RE = re.compile(
    r'(--[^\r\n]*|/\*.*?\*/)',
    re.DOTALL,
)


@dataclass(frozen=True)
class SQLValidationResult:
    '''
    Результат проверки SQL-файла.

    :param str | None query: SQL-код.
    :param bool is_valid: SQL корректен и может быть выполнен.
    :param bool is_empty: Файл не содержит SQL-кода.

    :example:
    `SQLValidationResult(query='SELECT 1', is_valid=True, is_empty=False)`
    '''

    query: str | None
    is_valid: bool
    is_empty: bool


def validate_sql_file(
    path: Path,
    dialect: str,
) -> SQLValidationResult:
    '''
    Читает и проверяет SQL-файл.

    :param Path path: Путь к SQL-файлу.
    :param str dialect: Диалект SQLGlot.
    :return SQLValidationResult: Результат проверки.

    :example:
    `validate_sql_file(Path('sql/core/01_database.sql'), 'clickhouse')` //=>
    `SQLValidationResult(query='...', is_valid=True, is_empty=False)`
    '''

    if not path.exists():
        log.error(
            'SQL-файл не найден: %s',
            path,
        )

        return SQLValidationResult(
            query=None,
            is_valid=False,
            is_empty=False,
        )

    try:
        query = path.read_text(
            encoding='utf-8',
        )

    except OSError as error:
        log.error(
            'Не удалось прочитать SQL-файл %s: %s',
            path,
            error,
        )

        return SQLValidationResult(
            query=None,
            is_valid=False,
            is_empty=False,
        )

    query_without_comments = _SQL_COMMENT_RE.sub(
        '',
        query,
    )

    if not query_without_comments.strip():
        log.warning(
            'SQL-файл не содержит SQL-кода, пропуск: %s',
            path.name,
        )

        return SQLValidationResult(
            query=None,
            is_valid=True,
            is_empty=True,
        )

    try:
        sqlglot.parse(
            query,
            read=dialect,
        )

    except ParseError as error:
        log.error(
            'Некорректный SQL в файле %s: %s',
            path,
            error,
        )

        return SQLValidationResult(
            query=None,
            is_valid=False,
            is_empty=False,
        )

    log.info(
        'SQL-файл прошел проверку: %s',
        path.name,
    )

    return SQLValidationResult(
        query=query,
        is_valid=True,
        is_empty=False,
    )