import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/api-request')

def safe_main_callable():
    from insert_records import main
    return main()

default_args = {
    # ...
    'start_date': datetime(2026, 1, 1),
    'catchup': False,
}

dag = DAG(
    dag_id='weather-api-orchestrator',
    default_args=default_args,
    schedule=timedelta(days=1),
    max_active_runs=1
)

with dag:
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=safe_main_callable
    )

    task2 = DockerOperator(
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        # PERBAIKAN UTAMA ADA DI SINI:
        mounts=[
            Mount(
                source='/root/repos/weather-data-report/dbt/my_project',
                target='/usr/app',
                type='bind' 
            ), 
            Mount(
                source='/root/repos/weather-data-report/dbt/profiles.yml',
                target='/root/.dbt/profiles.yml', 
                type='bind'
            )
        ],
        network_mode='weather-data-report_my-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    task1 >> task2