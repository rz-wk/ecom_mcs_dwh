'''
Пуш результатов dbt-тестов, не создаваемых dbt-коннектором OM.

Коннектор OpenMetadata не создаёт кейсы для singular-тестов, чьи
depends_on содержат одновременно source() и ref() (фактически в
проекте это reconciliation_* — сверки Core с производными слоями):
таблицу-носитель по такому узлу коннектор не определяет.

Скрипт закрывает дыру витрины: читает `run_results.json` полного
dbt build и пушит результаты тестов из белого списка PUSH_CASES
на явную таблицу-носитель через `dq.py` (REST, best-effort).
Путь к `run_results` тот же, что в `dbt/ingestion/dbt_core.yml`
(dbtRunResultsFilePath) — держить синхронно.

Правило сопровождения:
    - Если коннектор не создал кейс (теста нет в
    GET /dataQuality/testCases), то добавить имя в PUSH_CASES
    с таблицей-носителем;
    - Если после апгрейда OM коннектор начал
    создавать кейс сам -> убрать из списка, иначе будет дублирование.

:example:
`python -m src.lineage.emit_dbt_test_results
    --run-results dbt/target/process_dbt.dbt_test/run_results.json`
'''

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from src.lineage.dq import push
from src.utils.logging import configure_logging


log = logging.getLogger(__name__)

# Белый список: имя dbt-теста -> (таблица-носитель кейса в OM).
PUSH_CASES = {
    'reconciliation_core_marts': (
        'clickhouse_core.ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events',
    ),
    'reconciliation_core_purchases': (
        'clickhouse_core.ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events',
    ),
    'reconciliation_core_sessions': (
        'clickhouse_core.ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events',
    ),
}

# Dimension по подстроке имени — тот же словарь, что у патча
# коннектора (`docs/data_quality/naming.md`). Здесь только ключи,
# релевантные белому списку. Порядок = приоритет.
_DIMENSION_BY_SUBSTRING = (
    ('reconciliation', 'Consistency'),
    ('integrity', 'Integrity'),
    ('validity', 'Validity'),
    ('metrics', 'Accuracy'),
)


def _dimension(name: str) -> str:
    '''Подстрока имени -> dimension OM.'''

    for pattern, dimension in _DIMENSION_BY_SUBSTRING:
        if pattern in name:
            return dimension

    return ''


def _parse_args() -> argparse.Namespace:
    '''
    Разбирает аргументы командной строки.
    '''

    parser = argparse.ArgumentParser(
        description='Пуш результатов dbt-тестов в OM (белый список).',
    )

    parser.add_argument(
        '--run-results',
        type=Path,
        required=True,
        help='Путь к run_results.json прогона dbt build.',
    )

    return parser.parse_args()


def _load_results(path: Path) -> dict[str, dict]:
    '''
    Возвращает {имя_теста: узел result} для тестов белого списка.

    unique_id теста: test.<project>.<имя>.<hash>. Имя может
    содержать точки, поэтому извлекается всё между вторым
    сегментом и последним (хешем).
    '''

    raw = json.loads(path.read_text(encoding='utf-8'))

    results: dict[str, dict] = {}

    for node in raw.get('results', []):
        unique_id = node.get('unique_id', '')

        if not unique_id.startswith('test.'):
            continue

        # Имя теста = целый сегмент unique_id: устойчиво и к хешу
        # (4 сегмента), и к его отсутствию (3 сегмента), и к точкам
        # внутри имени проекта.
        segments = unique_id.split('.')

        for name in PUSH_CASES:
            if name in segments:
                results[name] = node

    return results


def main() -> None:
    '''
    Пушит результаты тестов белого списка в OM (best-effort).
    '''

    configure_logging()

    args = _parse_args()

    # Прогона не было (ранний сбой dbt): витрина не обновляется,
    # гейт уже отработал своим исключением на этапе process_dbt.
    if not args.run_results.exists():
        log.warning('run_results not found, skip: %s', args.run_results)
        return

    results = _load_results(args.run_results)

    for name, (table_fqn,) in PUSH_CASES.items():
        node = results.get(name)

        if node is None:
            # Тест не исполнялся в прогоне (upstream-модель упала
            # раньше): сигнализируем в лог, кейс не трогаем.
            log.warning('Test not found in run_results: %s', name)
            continue

        status = node.get('status')

        ok = status == 'pass'

        # value: статус + число нарушений + сообщение dbt.
        value = f'status={status}, failures={node.get("failures")}'

        message = node.get('message') or ''

        if message:
            value = f'{value}; {message}'

        push(
            table_fqn=table_fqn,
            case_name=name,
            ok=ok,
            value=value,
            description=(
                'Сверка Core с производным слоем (dbt singular). '
                'Кейс недоступен dbt-коннектору OM '
                '(source+ref в depends_on), пушится напрямую.'
            ),
            dimension=_dimension(name),
        )

    log.info('dbt test results pushed: tests=%s', len(results))


if __name__ == '__main__':
    main()