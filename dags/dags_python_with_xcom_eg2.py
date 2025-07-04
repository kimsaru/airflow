import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    @task(task_id="python_xcom_push_by_return")
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        v1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + v1)

    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값: ' + status)


    test = xcom_push_result()
    xcom_pull_2(test)
    test >> xcom_pull_1()