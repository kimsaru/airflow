from airflow import Dataset
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
import pendulum

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")
dataset_dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id="dags_dataset_consumer_2",
    schedule=[dataset_dags_dataset_producer_1, dataset_dags_dataset_producer_2],
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    tags=["testtest1"]
 ) as dag:
    
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo {{ ti.run_id }} && echo "producer_1 와 producer_2 이 완료되면 수행"',
    )