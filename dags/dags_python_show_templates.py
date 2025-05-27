import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    @task(task_id="print_task")
    def show_tempates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    print_task = show_tempates()

    print_task