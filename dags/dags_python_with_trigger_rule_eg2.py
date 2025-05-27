import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import random

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    @task.branch(task_id='random_branch')
    def random_branch():
        import random

        item_lst = ['A','B','C']
        select_item = random.choice(item_lst)

        if select_item == 'A':
            return 'task_a'
        elif select_item == 'B':
            return 'task_b'
        elif select_item == 'C':
            return 'task_c'
        
    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo upstream1",
    )

    @task(task_id="task_b")
    def task_b():
        print('정상처리')
    
    @task(task_id="task_c")
    def task_c():
        print('정상처리')
    
    @task(task_id="task_d", trigger_rule='none_skipped')
    def task_d():
        print('정상처리')
    
    random_branch() >> [task_a,task_b(),task_c()] >> task_d()