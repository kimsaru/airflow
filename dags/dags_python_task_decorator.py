import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    @task(task_id="print_task_1")
    def print_context(some_input):
        print(some_input)

    print_task_1 = print_context('task_decorator 실행')