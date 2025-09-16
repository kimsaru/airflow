from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago

def check_file_exists(response):
    json_data = response.json()
    return json_data.get("exists", False) is True

with DAG(
    dag_id='check_file_api_sensor',
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_file = HttpSensor(
        task_id='wait_file',
        http_conn_id='my_api_server',
        endpoint='check_file/file1.csv',  # 확인할 파일명
        response_check=check_file_exists,
        poke_interval=10,
        timeout=60,
        mode='reschedule',
    )