from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

def generate_values():
    # 길이가 가변적인 리스트
    return [[1, 2], [3, 4, 5], [6]]

def process_value(row):
    print("Row:", row)
    for v in row:
        print("Value:", v)

with DAG(
    dag_id="dags_gcp_operator_1",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    generate_values_task = PythonOperator(
        task_id="generate_values",
        python_callable=generate_values,
    )

    PythonOperator.partial(
        task_id="process_value",
        python_callable=process_value,
    ).expand(
        op_kwargs=[{"row": r} for r in generate_values_task.output]
    )
