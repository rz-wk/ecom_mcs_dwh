'''
Эмитер звена «dbt-модели» — рёбра между таблицами Core и Marts по графу dbt.

Почему вручную: cosmos-listener эмитит dbt-события со schema-facet без
типов колонок, а OM отвергает такие события (HTTP 400
"query param type must not be null"). Поэтому рёбра строятся из
`manifest.json` — это даёт полный depends_on-граф моделей и источников.

Схема работы
    manifest.json (nodes: models, sources: sources)
        -> для каждой model: output = таблица модели
        -> для каждого узла из depends_on: input = таблица model/source
        -> emit(inputs, outputs) по каждому ребру.

Маппинг dbt-баз на сервисы OM — _DB_TO_SERVICE.

Вызывается bash-таском `emit_dbt_lineage` дага после `process_dbt`:

    python -m src.lineage.emit_dbt \
        --manifest {_DBT_PROJECT_DIR}/target/manifest.json

Если manifest отсутствует — skip с ошибкой в лог (не роняет даг).
Manifest статичен: обновлять только при изменении графа моделей (`dbt parse`).
'''

import argparse, json, logging, os
from pathlib import Path

from config import _conf
from src.utils.logging import configure_logging

log = logging.getLogger(__name__)

from src.lineage.ol import emit

# Маппинг dbt database -> сервис OM (dbt-базы обе живут на одном CH-хосте,
# сервисы OM разделены по БД)
_DB_TO_SERVICE = {
    'ecom_mcs_dwh_core': 'clickhouse_core',
    'ecom_mcs_dwh_marts': 'clickhouse_marts'
}


def get_fqn(database, schema, ident):
    '''
    Преобразование тройки (database, schema, ident) из manifest.json
    в датасет формата: namespace = сервис OM, name = база.схема.таблица.

    Нюанс dbt-clickhouse: database может быть пустым или 'default' —
    тогда реальной базой считается schema.

    :param str database: database из manifest (может быть '' или 'default')
    :param str schema: схема из manifest
    :param str ident: физическое имя таблицы (alias или name модели / identifier источника)

    :return tuple: (namespace, name) для emit(), либо None, если база
                   не найдена в _DB_TO_SERVICE (модель вне наших БД)

    :example:
    `get_fqn('ecom_mcs_dwh_marts', 'ecom_mcs_dwh_marts', 'mart_sales')` //=>
    `('clickhouse_marts', 'ecom_mcs_dwh_marts.ecom_mcs_dwh_marts.mart_sales')`

    :example:
    `get_fqn('default', 'ecom_mcs_dwh_core', 'fact_events')` //=>
    `('clickhouse_core', 'ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events')`
    '''
    db = database if database and database != 'default' else schema
    service = _DB_TO_SERVICE.get(db)

    return (service, f'{db}.{schema}.{ident}') if service else None


def main():
    '''
    Точка входа: построение рёбер всех dbt-моделей из manifest.json.

    Шаги:
        1. Прочитать manifest; файла нет -> log.error, выход (skip).
        2. Для каждой node с resource_type == 'model':
           output = get_fqn(её database/schema/alias),
           inputs = get_fqn() по всем узлам depends_on
           (модели и sources; seed/snapshot -> None, отбрасываются).
        3. emit() по каждому ребру; счётчик обработанных моделей в лог.

    :param list argv: CLI-аргументы, ожидается --manifest <путь к manifest.json>

    :example:
    `python -m src.lineage.emit_dbt --manifest dbt/target/manifest.json` //=>
    `INFO dbt lineage emitted for 5 model(s)`
    (в OM: рёбра fact_events -> mart_sales, fact_events -> mart_users, ...)
    '''
    configure_logging()

    ap = argparse.ArgumentParser()
    ap.add_argument('--manifest', required=True)   # путь к dbt target/manifest.json
    path = ap.parse_args().manifest

    if not os.path.exists(path):
        log.error('manifest not found: %s (skipped)', path); return

    # m = json.loads(open(path).read())
    m = json.loads(Path(path).read_text(encoding='utf-8'))
    nodes, sources = m.get('nodes', {}), m.get('sources', {})

    def dep(uid):
        '''
        Датасет узла-зависимости из depends_on модели.

        Обрабатывает model- и source-узлы (seed/snapshot/прочее -> None,
        затем фильтруются вызывающим кодом).

        :param str uid: unique_id узла из manifest.json, напр. 'model.ecom_mcs_dwh.mart_sales'

        :return tuple: (namespace, name) для emit(), либо None

        :example:
        `dep('model.ecom_mcs_dwh.fact_events')` //=>
        `('clickhouse_core', 'ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events')`
        '''
        # Источник dbt: database/schema/identifier
        if uid.startswith('source.') and uid in sources:
            s = sources[uid]
            return get_fqn(
                s.get('database'),
                s.get('schema'),
                s.get('identifier')
            )

        # Модель dbt: database/schema/alias (alias приоритетнее name)
        if uid.startswith('model.') and uid in nodes:
            n = nodes[uid]
            return get_fqn(
                n.get('database'),
                n.get('schema'),
                n.get('alias') or n.get('name')
            )

        return None

    count = 0
    for uid, n in nodes.items():

        # Эмитим только модели (seeds/snapshots/tests пропускаем)
        if n.get('resource_type') != 'model':
            continue

        # Выход модели: её собственная таблица
        out = get_fqn(
            n.get('database'),
            n.get('schema'),
            n.get('alias') or n.get('name')
        )

        if not out:
            log.warning('skip %s: no service for db=%s schema=%s', uid, n.get('database'), n.get('schema'))
            continue

        # Входы: таблицы всех зависимостей (None-значения отфильтрованы)
        ins = [d for d in (dep(x) for x in n.get('depends_on', {}).get('nodes', [])) if d]

        emit(inputs=ins, outputs=[out])

        count += 1

    log.info('dbt lineage emitted for %s model(s)', count)

if __name__ == '__main__':
    main()