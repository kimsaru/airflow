import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="/opt/airflow/plugins/shell/test.sh test1",
    )

    bash_t2 = BashOperator(
        task_id="bash_t1",
        bash_command="/opt/airflow/plugins/shell/test.sh test2",
    )

    bash_t1 >> bash_t2