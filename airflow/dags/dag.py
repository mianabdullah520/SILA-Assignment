from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src.ingest import ingest_data
from src.transform import drop_columns, handle_missing_values, handle_outliers, normalize_text
from src.load import create_table_and_insert_data
from src.analyze import top_10_products_analysis

default_args = {
    'owner': 'Abdullah',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='An Airflow DAG to automate the data pipeline',
    schedule_interval='@daily',
)


def ingest_task():
    dataset_path = "data/Digital_Music.json"
    df = ingest_data(dataset_path)
    return df

ingest = PythonOperator(
    task_id='data_ingestion',
    python_callable=ingest_task,
    dag=dag,
)

def transform_task(df):
    df = drop_columns(df)
    df = handle_missing_values(df)
    df = handle_outliers(df)
    df = normalize_text(df)
    return df

transform = PythonOperator(
    task_id='data_transformation',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

def analysis_task(df):
    top_10_products_analysis(df)

analysis = PythonOperator(
    task_id='data_analysis',
    python_callable=analysis_task,
    provide_context=True,
    dag=dag,
)

def load_task(df):
    create_table_and_insert_data(df, 'reviews')

load = PythonOperator(
    task_id='data_loading',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

ingest >> transform >> analysis >> load
