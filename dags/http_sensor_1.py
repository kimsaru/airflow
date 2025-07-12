from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='http_sensor_1',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["testtest3"],
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    # 실패를 유도하기 위해 존재하지 않는 endpoint 사용
    failing_http_sensor = HttpSensor(
        task_id='failing_http_sensor',
        http_conn_id='my_api_server',
        endpoint='check_pin_file',
        method='GET',
        response_check=lambda response: response.json().get("file_exists") is True,
        poke_interval=30,
        timeout=600,
        mode='reschedule',
        soft_fail=True  # 실패를 DAG 실패로 반영 (중요!)
    )

    task_all_done = BashOperator(
        task_id='task_all_done',
        bash_command='echo "Runs regardless of upstream result"'
    )

    # DAG 구조
    start >> failing_http_sensor >> task_all_done