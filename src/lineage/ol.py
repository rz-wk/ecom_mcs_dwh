'''
OpenLineage emit + OM-сущности. 
Общий helper для ручного эмита lineage в OpenMetadata (OM).

OL-endpoint OM (`/api/v1/openlineage/lineage`) создаёт только рёбра между
уже существующими сущностями. Сами сущности (таблицы) создают
ingestion-коннекторы или REST API (PUT).

Ручной эмит использует формат — namespace и name, которые указывают
напрямую на FQN существующей сущности OM:

    namespace = <имя сервиса OM>          # postgres_ods, clickhouse_core, ...
    name      = <база.схема.таблица>      # хвост FQN без имени сервиса

    Пример
        таблица `clickhouse_core.ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events`
        -> namespace='clickhouse_core', name='ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events'

JDBC-формат namespace (postgres://host:port/db) не используется по причине
неоднозначности host:port (два CH-сервиса на localhost:8123) ребро ложится
на случайную копию таблицы.

Все операции best-effort: ошибки логируются, пайплайн не роняют.

Настройки берутся из env (OM_API, OM_LINEAGE_URL, OM_TOKEN) с fallback
в конфиг проекта (`om.api`, `om.lineage_url`, `om.token`).
'''

import os, uuid, datetime, logging, requests

from config import _conf
from src.utils.logging import configure_logging

log = logging.getLogger(__name__)

# Базовый URL REST API OpenMetadata (сущности: /databases, /tables, ...)
OM_API = os.getenv('OM_API', _conf.get('om.api'))
# Путь OL-endpoint'а (эмит событий lineage), присоединяется к OM_API
OM_LINEAGE_URL = os.getenv('OM_LINEAGE_URL', _conf.get('om.lineage_url'))
# JWT-токен бота ingestion (тот же, что в airflow.cfg [openlineage])
OM_TOKEN = os.getenv('OM_TOKEN', _conf.get('om.token'))

# Job, от имени которого эмитятся события: (namespace, имя дага Airflow).
# Должен совпадать с [openlineage] namespace airflow.cfg, чтобы рёбра
# привязывались к существующему pipeline в OM.
JOB = ('airflow_openlineage', 'ecom_mcs_dwh_daily_pipeline')
# Спецификация OpenLineage RunEvent
SPEC = _conf.get('om.spec')
# URI продюсера (источник события) — отображается в OM в деталях ребра
PRODUCER = _conf.get('om.producer')


def _h():
    '''
    Заголовок Authorization с JWT-токеном OM.

    :return dict: заголовки для requests
    '''
    return {'Authorization': f'Bearer {OM_TOKEN}'}


def emit(inputs, outputs):
    '''
    Эмит OpenLineage RunEvent: создаёт в OM рёбра lineage
    «каждый input -> каждый output» от имени джобы JOB.

    Важно
        обе стороны каждого ребра должны существовать в OM как сущности
        (см. формат в шапке модуля), иначе ребро не создастся
        (ответ OM: "no lineage edges created", HTTP 200 — ошибки нет).

    Best-effort 
        исключения логируются, исключение наружу не пробрасывается.

    :param list inputs: список входных датасетов [(namespace, name), ...]
    :param list outputs: список выходных датасетов [(namespace, name), ...]

    :example:
    `emit(inputs=[('csv_staging', 'raw.staging.2020-04')],`
    `     outputs=[('postgres_ods', 'ecom_mcs_dwh.ods.events')])` //=>
    `INFO OL emit: [('csv_staging', 'raw.staging.2020-04')] -> [('postgres_ods', 'ecom_mcs_dwh.ods.events')] ({"status":"success","message":"Created 1 lineage edge(s)",...})`
    '''
    try:
        event = {
            'eventType': 'COMPLETE',
            'eventTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'producer': PRODUCER,
            'schemaURL': SPEC,
            'run': {'runId': str(uuid.uuid4())},          # уникальный run на каждый эмит
            'job': {'namespace': JOB[0], 'name': JOB[1]},  # привязка рёбер к pipeline дага
            'inputs':  [{'namespace': ns, 'name': n} for ns, n in inputs],
            'outputs': [{'namespace': ns, 'name': n} for ns, n in outputs],
        }

        r = requests.post(
            f'{OM_API}{OM_LINEAGE_URL}',
            json=event,
            headers=_h(),
            timeout=10
        )
        r.raise_for_status()

        log.info('OL emit: %s -> %s (%s)', inputs, outputs, r.text[:120])

    except Exception:
        log.exception('OL emit failed (ignored)')


def register_csv_table(name, csv_path, columns):
    '''
    Регистрация csv-файла в OM как таблицы-представления
    `csv_staging.raw.staging.<name>` с колонками из заголовка csv.

    Создаёт (идемпотентно, PUT) иерархию сущностей:
        сервис csv_staging (создан заранее в UI, ingestion для него не нужен)
        -> database `raw`
           -> schema `staging`
              -> table `<name>` (tableType=External, все колонки STRING).

    Имя таблицы = стем файла без расширения и БЕЗ точек
    (точка — разделитель FQN в OM).

    Best-effort
        исключения логируются, наружу не пробрасываются.

    :param str name: имя таблицы (= стем csv-файла, напр. '2020-04')
    :param str csv_path: путь к csv-файлу (только для description таблицы)
    :param list columns: список имён колонок из заголовка csv

    :example:
    `register_csv_table('2020-04', 'data/raw/2020-04.csv',`
    `                    ['event_time', 'event_type', 'user_id'])` //=>
    `в OM создана таблица csv_staging.raw.staging.2020-04 с колонками event_time, event_type, user_id (все STRING); повторный вызов — обновление по FQN`
    '''
    try:
        # 1. database raw (в сервисе csv_staging) — создастся, если нет
        requests.put(
            f'{OM_API}/databases',
            json={'service': 'csv_staging', 'name': 'raw'},
            headers=_h(),
            timeout=10
        )
        # 2. схема staging — создастся, если нет
        requests.put(
            f'{OM_API}/databaseSchemas',
            json={'database': 'csv_staging.raw', 'name': 'staging'},
            headers=_h(),
            timeout=10
        )

        # 3. таблица-файл: PUT по FQN => создание или обновление (колонки, описание)
        body = {
            'databaseSchema': 'csv_staging.raw.staging',
            'name': name,
            'tableType': 'External',
            'description': f'CSV-файл {csv_path}',
            'columns': [{'name': c, 'dataType': 'STRING'} for c in columns],
        }

        r = requests.put(
            f'{OM_API}/tables',
            json=body,
            headers=_h(),
            timeout=10
        )
        r.raise_for_status()

    except Exception:
        log.exception('register_csv_table failed (ignored)')

if __name__ == '__main__':
    configure_logging()