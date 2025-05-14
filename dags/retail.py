# retail.py
from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig




@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
def check_load(scan_name='check_load', checks_subpath='sources'):
    try:
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)
    except Exception as e:
        import logging
        logging.exception("Soda check failed")
        raise



@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)
def retail():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='markt_online_retail',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )

    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://markt_online_retail/raw/online_retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_invoices',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
    )

    # ✅ 正确地定义 DbtTaskGrou
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    # ✅ 调用外部 Python 环境中 soda check 的任务
    soda_check = check_load()
    soda_check_transform = check_load(scan_name='check_transform', checks_subpath='transform')


    # ✅ 设置任务依赖
    upload_csv_to_gcs >> gcs_to_raw
    create_retail_dataset >> gcs_to_raw
    gcs_to_raw >> soda_check
    soda_check >> transform  # ⬅️ 正确方式，将 transform 作为 TaskGroup 使用
    transform >> soda_check_transform
    soda_check_transform >> report

# ✅ 注册 DAG 给 Airflow 调度器识别
retail_dag = retail()