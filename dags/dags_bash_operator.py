from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import datetime
import pendulum

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t3 = BashOperator(
        task_id="bash_t3",
        bash_command="echo $HOSTNAME",
    )

    bash_t4= BashOperator(
        task_id="bash_t4",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2 >> bash_t3 >> bash_t4