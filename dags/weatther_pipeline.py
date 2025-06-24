import sys
import os
sys.path.append('/home/sedera/airflow/dags/comparaison_climatique-exam')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract_weather_data import extract_data
from etl.transform_weather_data import transform_data
from etl.load_weather_data import load_data


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='weather_comparison_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval='@daily',  # exécuter tous les jours
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_data
    )

    extract >> transform >> load
