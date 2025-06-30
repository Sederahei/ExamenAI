import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importation du chemin du projet (inchangé)
dag_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(dag_dir, '..'))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

from etl.extract_weather_data import extract_data
from etl.transform_weather_data import transform_data
from etl.load_weather_data import load_data

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# --- DÉFINITION DES DATES DÉSIRÉES ---
# ATTENTION: Assurez-vous que ces dates sont valides par rapport à la date actuelle
# pour l'API Open-Meteo (historique pour le passé, prévision pour le futur proche).
# Si vous mettez des dates trop loin dans le futur pour la prévision, l'API ne les donnera pas.
TARGET_START_DATE = "2025-06-20" # La date de début que vous voulez
TARGET_END_DATE = "2025-07-07"   # La date de fin que vous voulez

with DAG(
    dag_id='weather_comparaison_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 6, 1), # La start_date du DAG peut rester la même, elle n'impacte pas directement la période des données extraites ici.
    schedule=None,                   # Mettez 'None' si vous voulez exécuter le DAG manuellement pour une période fixe.
                                     # Ou '@once' si vous voulez qu'il s'exécute une fois après sa start_date.
                                     # Si vous le laissez '@daily', il s'exécutera tous les jours, mais extraira toujours les mêmes dates fixes.
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_data,
        op_kwargs={
            'start_date_str': TARGET_START_DATE,
            'end_date_str': TARGET_END_DATE
        }
    )

    transform = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_data,
        op_kwargs={
            'start_date_str': TARGET_START_DATE,
            'end_date_str': TARGET_END_DATE
        }
    )

    load = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_data,
        op_kwargs={
            'start_date_str': TARGET_START_DATE,
            'end_date_str': TARGET_END_DATE
        }
    )

    extract >> transform >> load