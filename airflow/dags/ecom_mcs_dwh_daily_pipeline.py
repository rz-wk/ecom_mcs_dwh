'''
Основной DAG ETL-пайплайна ecom_mcs_dwh.

Airflow отвечает только за orchestration:
    - порядок выполнения задач;
    - передачу batch_date;
    - retries;
    - timeout;
    - остановку pipeline при ошибке
    …

Особенности:
    - daily incremental processing;
    - batch_date передается во все этапы;
    - зависимости между этапами задаются явно;
    - безопасный повторный запуск batch_date
      (идемпотентность PySpark и dbt incremental-моделей);
    - не содержит бизнес-логику преобразования данных.

Pipeline:

    ODS PostgreSQL
        |
    PySpark
        |
    Core ClickHouse
        |
    dbt dimensions / facts / marts
        |
    Marts ClickHouse
        |
    dbt tests
'''

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
import os

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Делаем модули проекта доступными для Airflow
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pendulum
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode, InvocationMode, TestBehavior
from cosmos.config import ExecutionConfig

from config import _conf

_BASH_PY_VERSION = _conf.get('airflow.bash_python_version')
# глобальный python (pyspark, эмиттеры)
_PY_EXEC_PATH = _conf.get('paths.python')
# airflow-venv (openmetadata-ingestion)
_AIRFLOW_PY_PATH = os.path.abspath(_PROJECT_ROOT / _conf.get('paths.airflow_python'))

# глобальный dbt (cosmos)
_DBT_EXEC_PATH = _conf.get('paths.dbt')

_SPARK_ENV = {
    'PYSPARK_PYTHON': _PY_EXEC_PATH,
    'PYSPARK_DRIVER_PYTHON': _PY_EXEC_PATH,
}

_DBT_PROJECT_PATH = _PROJECT_ROOT / _conf.get('airflow.dbt_project_dir')
_PROFILES_YML_PATH = _DBT_PROJECT_PATH / 'profiles.yml'

_DBT_TARGET_PATH = _DBT_PROJECT_PATH / 'target'
_MANIFEST_PATH = _DBT_TARGET_PATH / 'manifest.json'

@dag(
    dag_id=_conf.get('airflow.dag_id'),
    description=_conf.get('airflow.dag_desc'),
    tags=_conf.get('airflow.tags'),

    default_args={
        'owner': _conf.get('airflow.owner'),
        'depends_on_past': False,
        'retries': _conf.get('airflow.retries'),
        'retry_delay': timedelta(
            minutes=_conf.get('airflow.retry_delay_minutes'),
        ),
    },

    start_date=pendulum.parse(
        _conf.get('airflow.start_date'),
        tz=_conf.get('airflow.timezone'),
    ),
    # end_date=pendulum.parse(
    #     _conf.get('airflow.end_date'),
    #     tz=_conf.get('airflow.timezone'),
    # ),
    schedule=_conf.get('airflow.schedule'),
    catchup=_conf.get('airflow.catchup'),
    max_active_runs=_conf.get('airflow.max_active_runs'),
    dagrun_timeout=timedelta(
        hours=_conf.get('airflow.dagrun_timeout_hours'),
    ),
)
def ecom_mcs_dwh():

    # 1. Инициализация ODS
    # Разово, только до первого исторического бекфила
    # или до первого дневного батча

    @task.bash
    def init_ods() -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.db.init_ods'
        )

    # 2. CSV -> ODS

    @task.bash
    def process_csv_ods(ds=None) -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.ingestion.load_ods '
            f'--batch-date "{ds}" '
            f'--source-file "data/raw/{ds[:7]}.csv" '
            f'--load-mode {_conf.get("airflow.ods_load_mode", "day")} '
            '--resume'
        )

    # 2a. Проверка CSV -> ODS

    @task.bash
    def test_csv_ods(ds=None) -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.validation.csv_ods_check '
            f'--batch-date "{ds}" '
            f'--source-file "data/raw/{ds[:7]}.csv"'
        )

    # 3. Инициализация ODS индексов
    # Разово, только после первого исторического бекфила

    @task.bash
    def init_ods_indexes() -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.db.init_ods --stage indexes' 
        )

    # 4. Инициализация Core
    # Разово, только до первого исторического бекфила
    # или до первого дневного батча

    @task.bash
    def init_core() -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.db.init_core'
        )

    # 5. ODS -> Core

    @task.bash(env=_SPARK_ENV)
    def process_ods_core(ds=None) -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.pyspark.deduplicate_events '
            f'--batch-date "{ds}" '
            f'--source-file "{ds[:7]}.csv"'
        )

    # 6. Проверка ODS -> Core

    @task.bash(env=_SPARK_ENV)
    def test_ods_core(ds=None) -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.validation.integration_check '
            f'--batch-date "{ds}" '
            f'--source-file "{ds[:7]}.csv"'
        )

    # 7. Полный dbt pipeline

    # @task.bash(retries=0)
    # def process_dbt(ds=None) -> str:
    #     return (
    #         f'cd {_DBT_PROJECT_PATH} && '
    #         f'{_conf.get("airflow.dbt_command")} build '
    #         f'--vars \'{{"batch_date": "{ds}"}}\''
    #     )

    process_dbt = DbtTaskGroup(
        group_id='process_dbt',
        project_config=ProjectConfig(
            dbt_project_path=_DBT_PROJECT_PATH,
            manifest_path=_MANIFEST_PATH,
            dbt_vars={'batch_date': "{{ ds }}"}
            # Без Jinja здесь, вместо operator_args
            # env_vars={
            #     'DBT_TARGET_PATH': _DBT_PROJECT_PATH / 'target'
            # }
        ),
        profile_config=ProfileConfig(
            profile_name='ecom_mcs_dwh',
            target_name='local',
            profiles_yml_filepath=_PROFILES_YML_PATH
        ),
        # Убираем из парсинга dbt (рекомендация Cosmos),
        # в project_config указываем путь до manifest
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_ALL
            # load_method=LoadMode.DBT_LS,
            # dbt_executable_path=_DBT_EXEC_PATH
        ),
        # глобальный dbt (cosmos)
        # (удалить если dbt-clickhouse установить в airflow venv)
        execution_config=ExecutionConfig(
            dbt_executable_path=_DBT_EXEC_PATH,
            invocation_mode=InvocationMode.SUBPROCESS
        ),
        operator_args={
            # изолированный target для задач.   
            # задачи перестают писать в общий манифест,
            # чтобы не возникало конфликта рендеринга и записи
            'dbt_cmd_flags': ['--target-path', f'{_DBT_PROJECT_PATH}/target/{{{{ ti.task_id }}}}']
            # Перенесено vars в ProjectConfig
            # В operator_args наличие vars depricated.
            # 'vars': {'batch_date': "{{ ds }}"},
        },
        default_args={
            'retries': 0
        },
    )

    # Обновление манифеста для рендера
    @task.bash(retries=0)
    def refresh_dbt_manifest() -> str:
        return f'cd {_DBT_PROJECT_PATH} && {_DBT_EXEC_PATH} parse'

    # 8. OpenMetadata Lineage

    @task.bash
    def emit_csv_lineage(ds=None) -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.lineage.emit_csv '
            f'--source-file "data/raw/{ds[:7]}.csv"'
        )

    @task.bash
    def emit_spark_lineage() -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.lineage.emit_spark'
        )

    @task.bash
    def emit_dbt_lineage() -> str:
        return (
            f'cd {_PROJECT_ROOT} && ' 
            f'{_BASH_PY_VERSION} -m src.lineage.emit_dbt '
            f'--manifest {_DBT_PROJECT_PATH}/target/manifest.json'
        )

    @task.bash
    def ingest_dbt_tests() -> str:
        return (
            f'cd {_PROJECT_ROOT} && {_AIRFLOW_PY_PATH} -m metadata ingest -c dbt/ingestion/dbt_core.yml && '
            f'cd {_PROJECT_ROOT} && {_AIRFLOW_PY_PATH} -m metadata ingest -c dbt/ingestion/dbt_marts.yml'
        )

    # 9. Push dbt-тестов, недоступных коннектору OM

    @task.bash
    def push_dbt_test_results() -> str:
        return (
            f'cd {_PROJECT_ROOT} && '
            f'{_BASH_PY_VERSION} -m src.lineage.emit_dbt_test_results '
            # Путь синхронен dbtRunResultsFilePath в `dbt/ingestion/*.yml``
            f'--run-results {_DBT_PROJECT_PATH}/target/process_dbt.dbt_test/run_results.json'
        )

    (
        # init_ods()
        process_csv_ods() >> emit_csv_lineage() 
        >> test_csv_ods()
        # >> init_ods_indexes()
        # >> init_core()
        >> process_ods_core() >> emit_spark_lineage()
        >> test_ods_core()
        >> refresh_dbt_manifest() >> process_dbt 
        >> emit_dbt_lineage() >> ingest_dbt_tests()
        >> push_dbt_test_results()
    )

ecom_mcs_dwh()