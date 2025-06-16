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
    dag_id='http_sensor_trigger_rule_test',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["testtest2"],
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    # 실패를 유도하기 위해 존재하지 않는 endpoint 사용
    failing_http_sensor = HttpSensor(
        task_id='failing_http_sensor',
        http_conn_id='non_existent_connection',  # 없는 커넥션 ID
        endpoint='fake/endpoint',
        method='GET',
        response_check=lambda response: "OK" in response.text,
        poke_interval=5,
        timeout=10,
        mode='poke',
        soft_fail=False  # 실패를 DAG 실패로 반영 (중요!)
    )

    # 후속 태스크들
    task_all_success = BashOperator(
        task_id='task_all_success',
        bash_command='echo "Runs only if all upstream succeeded"'
    )

    # DAG 구조
    start >> failing_http_sensor >> task_all_success