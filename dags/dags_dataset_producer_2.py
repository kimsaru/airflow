from airflow import Dataset
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id="dags_dataset_producer_2",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    tags=["testtest1"]
 ) as dag:
    
    bash_task = BashOperator(
        task_id="bash_task",
        outlets=[dataset_dags_dataset_producer_2],
        bash_command='echo "producer_2 수행 완료"',
    )