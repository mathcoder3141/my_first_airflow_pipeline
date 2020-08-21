from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
from airflow.hooks.postgres_hook import PostgresHook
import json
import numpy as np

def load_data(**kwargs):
    """
    Processes the json data, checks the types and enters into the Postgres database
    """

    pg_hook = PostgresHook(postgres_conn_id='weather_id')

    file_name = str(datetime.now().date()) + '.json'
    tot_name = os.path.join(os.path.dirname(__file__), 'src/data', file_name)

    with open(tot_name, 'r') as inputfile:
        doc = json.load(inputfile)

    city = str(doc["name"])
    country = str(doc["sys"]["country"])
    latitude = float(doc["coord"]["lat"])
    longitude = float(doc["coord"]["lon"])
    sunrise = int(doc["sys"]["sunrise"])
    sunset = int(doc["sys"]["sunset"])
    wind_speed = float(doc["wind"]["speed"])
    humidity = int(doc["main"]["humidity"])
    pressure = int(doc["main"]["pressure"])
    min_temp = float(doc["main"]["temp_min"])
    max_temp = float(doc["main"]["temp_max"])
    temp = float(doc["main"]["temp"])
    weather = str(doc["weather"][0]["description"])
    created = datetime.now()

    valid_data = True
    for valid in np.isnan([latitude, longitude, sunrise, sunset, wind_speed, humidity, pressure, min_temp, max_temp,
                           temp]):
        if valid is False:
            valid_data = False
            break

    row = (city, country, latitude, longitude, sunrise, sunset, wind_speed, humidity, pressure, min_temp,
           max_temp, temp, weather, created)

    insert_cmd = """INSERT INTO dallas_weather
                    (city, country, latitude, longitude, sunrise, sunset, wind_speed, humidity, pressure, 
                    min_temp, max_temp, temp, weather, created)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    if valid_data is True:
        pg_hook.run(insert_cmd, parameters=row)

default_args = {
    'owner': 'Randall',
    'depends_on_past': False,
    'email': ['randallhall@icloud.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }


dag = DAG(
     'weatherDag',
     default_args=default_args,
     description="A simple DAG to get weather for Dallas via OpenWeather's API",
     start_date=datetime(2020, 8, 20),
     schedule_interval='@hourly'
)

task1 = BashOperator(
    task_id='create_database_and_table',
    bash_command='python ~/airflow/dags/src/makeTable.py',
    dag=dag
)

task2 = BashOperator(
     task_id='get_weather',
     bash_command='python ~/airflow/dags/src/getWeather.py',
     dag=dag)

task3 = PythonOperator(
     task_id='transform_load',
     provide_context=True,
     python_callable=load_data,
     dag=dag)

task1 >> task2 >> task3
