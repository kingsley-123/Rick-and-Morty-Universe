from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    "start_date": datetime(2024, 12, 22),
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
}

def episode():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from loaddata import load_episode_data
    return load_episode_data()

def location():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from loaddata import load_location_data
    return load_location_data()

def character():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from loaddata import load_character_data
    return load_character_data()

with DAG(
    dag_id="rickmorty",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="api",
        endpoint="api/",
        timeout=20,  # Add timeout to prevent hanging
        poke_interval=5,  # Check every 5 seconds
    )

    load_episode = PythonOperator(
        task_id='load_episode',
        python_callable=episode
    )
    
    load_location = PythonOperator(
        task_id='load_location',
        python_callable=location
    )
    
    load_character = PythonOperator(
        task_id='load_character',
        python_callable=character
    )

    is_api_available >> load_episode >> load_location >> load_character