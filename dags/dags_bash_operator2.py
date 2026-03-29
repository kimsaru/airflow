from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def succeed_task(task_name, **context):
    run_id = context['run_id']
    print(f"[{run_id}] Task {task_name} succeeded!")

def fail_task(task_name, **context):
    run_id = context['run_id']
    print(f"[{run_id}] Task {task_name} failed intentionally!")
    raise ValueError("Intentional failure for testing")

default_args = {
    'depends_on_past': False,
    'wait_for_downstream': True,
    'max_active_runs' : 1,
    'retries': 0
}

with DAG(
    dag_id='ttest2',
    default_args=default_args,
    start_date=datetime(2026, 3, 25),
    schedule_interval='@daily',
    catchup=True
) as dag:

    # Task 1: 성공
    t1 = PythonOperator(
        task_id='t1_success',
        python_callable=succeed_task,
        op_kwargs={'task_name': 't1_success'}
    )

    # Task 2: 실패
    t2 = PythonOperator(
        task_id='t2_fail',
        python_callable=fail_task,
        op_kwargs={'task_name': 't2_fail'}
    )

    # Task 3: downstream
    t3 = PythonOperator(
        task_id='t3_downstream',
        python_callable=succeed_task,
        op_kwargs={'task_name': 't3_downstream'}
    )

    # Task 순서 정의
    t1 >> t2 >> t3