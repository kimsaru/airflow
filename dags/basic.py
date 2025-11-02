from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_time():
    print("This runs every 10 minutes!")
    import cryptography
    print(cryptography.__version__)

with DAG(
    dag_id="basic",
    start_date=datetime(2023, 1, 1),
    schedule_interval="50 * * * *",  # 10분마다
    catchup=False,
    tags=["example"],
) as dag:

    task = PythonOperator(
        task_id="print_task",
        python_callable=print_time,
    )
