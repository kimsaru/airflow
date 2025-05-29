from airflow.models.dag import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

import datetime
import pendulum

with DAG(
    dag_id="dags_time_sensor_with_async",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2025, 5, 26, 0, 0, 0),
    end_date=pendulum.datetime(2025, 5, 26, 1, 0, 0),
    catchup=True,
    tags=["testtest1"]
) as dag:
    
    async_sensor = DateTimeSensorAsync(
        task_id = "async_sensor",
        target_time=""" {{ macros.datetime.utcnow() + macros.timedelta(minutes=5)}}"""
    )