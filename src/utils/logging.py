'''
Общие функции настройки логирования проекта.
'''

import logging

from config import _conf


def configure_logging() -> None:
    '''
    Настраивает стандартное логирование проекта.

    :example:
    `configure_logging()` //=>
    `INFO-сообщения выводятся в стандартный поток`
    '''

    logging.basicConfig(
        level=_conf.get('logging.level', 'INFO'),
        format=_conf.get('logging.format'),
        datefmt=_conf.get('logging.datefmt'),
    )