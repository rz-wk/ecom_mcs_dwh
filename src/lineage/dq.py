'''
Push результатов внешних DQ-проверок в OpenMetadata (OM 1.13.x).

Контракт по мануалу:
    PUT  /dataQuality/testDefinitions            — create/update,
         dimension полем dataQualityDimension;
    PUT  /dataQuality/testCases                  — upsert кейса
         (testSuite сервер назначает сам);
    POST /dataQuality/testCases/testCaseResults/{fqn} — результат.

FQN кейса = <FQN таблицы>.<имя кейса>; берётся из ответа сервера.

Dimension задаётся вызывающей стороной явно, тот же словарь
«тип: dimension», что у dbt-коннектора (docs/data_quality/naming.md):
Completeness / Validity / Uniqueness / Integrity / Consistency / Accuracy.

Best-effort: ошибки логируются, пайплайн не роняют.
Гейт остаётся за Airflow-таском, OM — витрина.
'''

import time
import logging

import requests

from src.lineage.ol import OM_API, _h

log = logging.getLogger(__name__)

_OK = 'Success'
_FAIL = 'Failed'


def _ensure_definition(
    name: str,
    description: str = '',
    dimension: str = '',
) -> None:
    '''
    Создает/обновляет TestDefinition (PUT, идемпотентен).
    '''

    body = {
        'name': name,
        'displayName': name,
        'description': description,
        'entityType': 'TABLE',
        'testPlatforms': ['OpenMetadata'],
    }

    if dimension:
        body['dataQualityDimension'] = dimension

    r = requests.put(
        f'{OM_API}/dataQuality/testDefinitions',
        json=body,
        headers=_h(),
        timeout=10,
    )

    if not r.ok:
        log.warning('OM testDefinitions: %s %s', r.status_code, r.text[:300])

    r.raise_for_status()


def _ensure_case(
    table_fqn: str,
    case_name: str,
    description: str = '',
) -> str:
    '''
    Создает/обновляет TestCase (PUT-upsert) и возвращает его FQN.

    Тело по контракту мануала: name, testDefinition, entityLink,
    parameterValues, description. testSuite сервер назначает сам.

    :param str table_fqn: FQN таблицы-носителя.
    :param str case_name: имя кейса.
    '''

    r = requests.put(
        f'{OM_API}/dataQuality/testCases',
        json={
            'name': case_name,
            'testDefinition': case_name,
            'entityLink': f'<#E::table::{table_fqn}>',
            'parameterValues': [],
            'description': description,
        },
        headers=_h(),
        timeout=10,
    )

    if not r.ok:
        log.warning('OM testCases: %s %s', r.status_code, r.text[:300])

    r.raise_for_status()

    # FQN из ответа сервера
    return r.json().get('fullyQualifiedName') or f'{table_fqn}.{case_name}'


def push(
    table_fqn: str,
    case_name: str,
    ok: bool,
    value: str,
    description: str = '',
    dimension: str = '',
) -> None:
    '''
    Пушит исход одной проверки в OM (best-effort).

    :param str table_fqn: FQN таблицы-носителя.
    :param str case_name: имя кейса.
    :param bool ok: исход проверки.
    :param str value: факт для 'result' и testResultValue.
    :param str dimension: OM тип проверки (Completeness|Validity|Uniqueness|Integrity|Consistency|Accuracy|'' — не задавать).
    '''

    try:
        _ensure_definition(case_name, description, dimension)

        case_fqn = _ensure_case(table_fqn, case_name, description)

        body = {
            'timestamp': int(time.time() * 1000),
            'testCaseStatus': _OK if ok else _FAIL,
            'result': str(value),
            'testResultValue': [{'name': 'value', 'value': str(value)}],
        }

        r = requests.post(
            f'{OM_API}/dataQuality/testCases/testCaseResults/{case_fqn}',
            json=body,
            headers=_h(),
            timeout=10,
        )

        if not r.ok:
            log.warning(
                'OM testCaseResults: %s %s',
                r.status_code,
                r.text[:300],
            )

        r.raise_for_status()

        log.info(
            'DQ push: %s.%s = %s',
            table_fqn,
            case_name,
            body['testCaseStatus'],
        )

    except Exception:
        log.exception('DQ push failed (ignored): %s.%s', table_fqn, case_name)


def push_checks(checks: list[dict]) -> None:
    '''
    Пушит список проверок (best-effort).

    Элемент: {table, name, ok, value, dimension (опц.),
    description (опц.)}.
    '''

    for check in checks:
        push(
            table_fqn=check['table'],
            case_name=check['name'],
            ok=check['ok'],
            value=check['value'],
            description=check.get('description', ''),
            dimension=check.get('dimension', ''),
        )