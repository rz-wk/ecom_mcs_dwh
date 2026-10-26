'''
Эмитер этапа «csv -> Postgres ODS».

Регистрирует csv-файл месяца в OM как таблицу csv_staging.raw.staging.<ГГГГ-ММ>
(колонки = заголовок csv) и эмитит ребро
`csv_staging.raw.staging.<ГГГГ-ММ> -> postgres_ods.ecom_mcs_dwh.ods.events`.

Вызывается bash-таском `emit_csv_lineage` дага сразу после `process_csv_ods`:

    python -m src.lineage.emit_csv \
        --source-file "data/raw/{ds[:7]}.csv"

Если csv-файла нет (backfill по месяцу без данных) — молчаливый skip
(без файла нет ни сущности, ни ребра).
'''

import argparse, csv, logging
from pathlib import Path

from config import _conf
from src.utils.logging import configure_logging

log = logging.getLogger(__name__)

from src.lineage.ol import emit, register_csv_table


def main():
    '''
    Точка входа: csv -> таблица OM + ребро csv -> ods.events.

    Шаги:
        1. Прочитать заголовок csv (первая строка) -> список колонок.
           Файла нет / пустой -> log.exception, выход (skip).
        2. register_csv_table(): создать/обновить таблицу-представление в OM.
        3. emit(): ребро в OM.

    :param list argv: CLI-аргументы, ожидается --source-file <путь к csv>

    :example:
    `python -m src.lineage.emit_csv --source-file "data/raw/2020-04.csv"` //=>
    `INFO OL emit: [('csv_staging', 'raw.staging.2020-04')] -> [('postgres_ods', 'ecom_mcs_dwh.ods.events')] ({"status":"success","message":"Created 1 lineage edge(s)",...})`
    '''
    configure_logging()

    ap = argparse.ArgumentParser()
    ap.add_argument('--source-file', required=True)   # путь к csv относительно корня проекта

    path = Path(ap.parse_args().source_file)
    name = path.stem  # 2019-10.csv -> 2019-10 (имя таблицы; точек быть не должно)

    try:
        # Заголовок csv = список колонок будущей таблицы OM
        with path.open(newline='') as f:
            header = next(csv.reader(f))
    except Exception:
        log.exception('CSV header read failed (ignored)')
        return

    register_csv_table(name, str(path), header)

    # Ребро: csv-файл -> ODS-таблица (формат: namespace = сервис OM)
    emit(
        inputs=[(
            'csv_staging',
            f'raw.staging.{name}'
        )],
        outputs=[(
            'postgres_ods',
            'ecom_mcs_dwh.ods.events'
        )]
    )

if __name__ == '__main__':
    main()