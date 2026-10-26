'''
Общие настройки проекта.

Файл не содержит учетных данных. Локальные настройки и credentials находятся в `config_local.py`.
'''

from pathlib import Path


_conf = {
    'ingestion': {
        # 'chunk_size': 100_000, # строк на батч чтения (block_size reader'а)
        'read_block_mb': 32,  # байтовый блок reader'а
        'copy_workers': 2,    # параллельные COPY-воркеры 2,4
        'copy_buffer_mb': 32, # буфер передачи в COPY
    },

    'validation': {
        # Порог политики rows_rejected
        # `fail` при `max_rows_rejected > N`
        'max_rows_rejected': 0
    },
    
    'paths': {
        'data_raw': Path('data/raw'),
        'quarantine': Path('data/quarantine'),
        'staging': Path('data/staging'),

        # Полный path до глобального Python
        'python': Path('/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14'),
        # Полный path до глобального dbt
        'dbt': Path('/Library/Frameworks/Python.framework/Versions/3.14/bin/dbt'),

        'airflow_python': '../airflow/venv/bin/python'
    },

    'logging': {
        'level': 'INFO',
        'format': (
            '%(asctime)s | %(levelname)s | '
            '%(name)s | %(message)s'
        ),
        'datefmt': '%Y-%m-%d %H:%M:%S',
    },
    
    'profiler': {
        'timer': {
            'report': False
        }
    },

    'databases': {
        'postgres_ods': {
            'driver': 'postgresql+psycopg',
            'host': 'localhost',
            'port': 5432,
            'database': 'ecom_mcs_dwh',
        },

        'clickhouse_core': {
            'driver': 'clickhouse+http',
            'host': 'localhost',
            'port': 9005,
            # 'port': 8123,
            'database': 'ecom_mcs_dwh_core',
        },

        'clickhouse_marts': {
            'driver': 'clickhouse+http',
            'host': 'localhost',
            'port': 9005,
            'database': 'ecom_mcs_dwh_marts',
        },
    },

    'spark': {
        # Использовать 2 локальных worker threads 
        # вместо всех доступных ядер
        'master': 'local[2]',
        'driver_cores': 2,
        'shuffle_partitions': 2,

        'session_timezone': 'UTC',

        'jdbc': {
            'driver': 'org.postgresql.Driver',
            'fetchsize': 10_000,
            'package': 'org.postgresql:postgresql:42.7.7',
            'num_partitions': 8,
        },

        'clickhouse': {
            'insert_batch_size': 50_000,
            'write_partitions': 12,
        },
    },

    'om': {
        # Базовый URL REST API OpenMetadata
        'api': 'http://127.0.0.1:8585/api/v1',
        # Путь OL-endpoint'а (эмит событий lineage), присоединяется к OM_API
        'lineage_url': '/openlineage/lineage', 
        # Спецификация OpenLineage RunEvent
        'spec': 'https://openlineage.io/spec/2-0-2/OpenLineage.json#definition/RunEvent',
        # URI продюсера (источник события) — отображается в OM в деталях ребра
        'producer': 'file://src/lineage/ol.py'
    },
    
    'airflow': {

        # [tasks]
        # Режим загрузки: один день или весь месяц одним проходом (day, month)
        'ods_load_mode': 'day',
        # Интерпретатор Python для запуска проектных модулей
        'bash_python_version': 'python3.14',
        # Команда запуска dbt
        'dbt_command': 'dbt',
        # Директория dbt-проекта
        # /opt/ecom_mcs_dwh для Docker
        'dbt_project_dir': 'dbt',
        # dbt-модели для последовательного запуска
        'dbt_models': { 
            'dimensions': (
                'dim_user dim_product dim_category'
            ),
            'facts': (
                'fact_sessions fact_purchases'
            ),
            'marts': 'marts',
        },
        
        # [DAG]
        # Идентификатор DAG
        'dag_id': 'ecom_mcs_dwh_daily_pipeline',
        # Описание
        'dag_desc': 'Daily ODS → PySpark → Core → dbt → Marts pipeline',
        # Теги
        'tags': ['ecom', 'dwh', 'etl'],

        # [default_args]
        'owner': 'rzwk',
        # Количество повторных попыток failed task
        'retries': 2,
        # Интервал между повторными попытками task, минуты
        'retry_delay_minutes': 1,

        # [scheduler]
        # Дата начала планового запуска DAG
        'start_date': '2019-10-01',
        # Дата окончания запуска DAG
        # Если нужно ограничить исторические запуски
        # 'end_date': '2019-04-30',
        # Часовой пояс Airflow DAG
        'timezone': 'UTC',
        # Запускать исторические невыполненные интервалы DAG при его активации
        # False — игнорировать прошлые периоды с start_date до сегодня
        'catchup': False,
        # Расписание ежедневного запуска DAG в формате: 
        # - cron: '0 3 * * *' каждый день в 03:00; 
        # - макросы: @hourly, @daily, @weekly, None; 
        # - объекты timedelta: timedelta(minutes=15) строго каждые 15 минут от старта.
        'schedule': '0 3 * * *',
        # 'schedule': None, # Для исторических данных
        # Максимальное количество одновременно выполняемых DAG runs
        'max_active_runs': 1,
        # Максимальное время выполнения одного DAG run, часы
        'dagrun_timeout_hours': 2,
    },
}