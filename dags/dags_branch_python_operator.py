import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import random

with DAG(
    dag_id="dags_branch_python_operator",
    schedule='0 1 * * *',
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    def select_random():
        import random

        item_lst = ['A','B','C']
        select_item = random.choice(item_lst)

        if select_item == 'A':
            return 'task_a'
        elif select_item in ['B','C']:
            return ['task_b', 'task_c']

    python_branch_task = BranchPythonOperator(
        task_id = 'python_branch_task',
        python_callable=select_random
    )

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]
