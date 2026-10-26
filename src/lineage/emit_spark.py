'''
Эмитер этапа «Postgres ODS -> PySpark -> ClickHouse Core».

PySpark-этап непрозрачен для автоматических lineage-экстракторов
(чтение jdbc + mapInPandas в dbt-задачу не отслеживается), поэтому ребро
`postgres_ods...ods.events -> clickhouse_core...fact_events`
эмитится вручную. Список входов/выходов фиксирует фактическую логику
модуля src.pyspark.deduplicate_events.

Ребро эмитится на уровне модуля — выполняется при запуске
`python -m src.lineage.emit_spark` (вызывается bash-таском
`emit_spark_lineage` дага сразу после `process_ods_core`).

Расширение: при добавлении новых таблиц на этом этапе дополнить
inputs/outputs ниже (парами (сервис, база.схема.таблица)).
'''

import logging

from config import _conf
from src.utils.logging import configure_logging

log = logging.getLogger(__name__)

from src.lineage.ol import emit


def main():
    '''
    Эмит ребра ODS -> Core (см. докстринг модуля).
    '''

    configure_logging()

    # Ребро: ODS-таблица -> таблица Core (формат: namespace = сервис OM)
    emit(
        inputs=[('postgres_ods', 'ecom_mcs_dwh.ods.events')],
        outputs=[(
            'clickhouse_core',
            'ecom_mcs_dwh_core.ecom_mcs_dwh_core.fact_events',
        )],
    )


if __name__ == '__main__':
    main()