from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src import extract, load, transform

def run_extract(**kwargs):
    return extract.run_all()

def run_load(**kwargs):
    ti = kwargs['ti']
    data_frames = ti.xcom_pull(task_ids='extract_task')
    load.run_all(data_frames=data_frames)

def run_transform(**kwargs):
    transform.run_all()

default_args = {
    'owner': 'Carlos Gomez',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='elt_pipeline_weekly',
    default_args=default_args,
    description='Simulación de pipeline semanal con Airflow',
    schedule_interval='@weekly',
    start_date=datetime(2025, 10, 6),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=run_load
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform
    )

    extract_task >> load_task >> transform_task
