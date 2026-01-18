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

    def load_bq_callable():
        from postgres_to_bq import load_data_to_bq
        load_data_to_bq()

    task2 = PythonOperator(
        task_id='load_to_bq_task',
        python_callable=load_bq_callable
    )

    task3 = DockerOperator(
        task_id='transform_data_bigquery',
        image='ghcr.io/dbt-labs/dbt-bigquery:1.9.latest',
        entrypoint='/bin/bash',
        command='-c "echo DEBUG: GCP_PROJECT_ID=$GCP_PROJECT_ID && dbt run --target prod"',
        working_dir='/usr/app',
        mounts=[
            Mount(source='D:/etl/weather-data-etl/dbt/my_project', target='/usr/app', type='bind'), 
            Mount(source='D:/etl/weather-data-etl/dbt/profiles.yml', target='/root/.dbt/profiles.yml', type='bind'),
            Mount(source='D:/etl/weather-data-etl/dbt/service-account.json', target='/root/.dbt/service-account.json', type='bind')
        ],
        environment={
            'GOOGLE_APPLICATION_CREDENTIALS': '/root/.dbt/service-account.json',
            'GCP_PROJECT_ID': os.getenv('GCP_PROJECT_ID'), 
            'BQ_DATASET': 'weather_data',
            'DBT_PROFILES_DIR': '/root/.dbt',
            'DBT_SOURCE_DATABASE': os.getenv('GCP_PROJECT_ID'),
            'DBT_SOURCE_SCHEMA': 'weather_data'
        },
        network_mode='weather-data-etl_my-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
        mount_tmp_dir=False
    )

    task1 >> task2 >> task3