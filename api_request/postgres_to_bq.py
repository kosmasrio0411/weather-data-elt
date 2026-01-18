import os
import psycopg2
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def connect_to_postgres():
    """Connect to the PostgreSQL database."""
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'db'),
        port=5432,
        dbname=os.getenv('POSTGRES_DB', 'db'),
        user=os.getenv('POSTGRES_USER', 'db_user'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    return conn

def get_bigquery_client():
    print("Creating BigQuery client...")

    key_path = '/opt/airflow/api-request/service-account.json'
    

    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        key_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if not os.path.exists(key_path):
        key_path = 'd:/etl/weather-data-etl/dbt/service-account.json'

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

def load_data_to_bq():
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET')
    table_id = f"{project_id}.{dataset_id}.raw_weather_data"

    pg_conn = None
    try:

        pg_conn = connect_to_postgres()
        query = "SELECT * FROM dev.raw_weather_data"
        print("Fetching data from Postgres...")
        df = pd.read_sql(query, pg_conn)
        print(f"Fetched {len(df)} rows.")

        if df.empty:
            print("No data to load.")
            return

        client = get_bigquery_client()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", 
        )

        print(f"Loading data to BigQuery table: {table_id}...")
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete.

        print("Loaded {} rows and {} columns to {}".format(
            df.shape[0], df.shape[1], table_id
        ))

    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
        raise
    finally:
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    load_data_to_bq()
