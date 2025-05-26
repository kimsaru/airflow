from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import datetime
import pendulum

with DAG(
    dag_id="dags_conn_test",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 25, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    t1 = EmptyOperator(
        task_id="t1"
    )

    t2 = EmptyOperator(
        task_id="t2"
    )

    t3 = EmptyOperator(
        task_id="t3"
    )

    t4 = EmptyOperator(
        task_id="t4"
    )

    t5 = EmptyOperator(
        task_id="t5"
    )

    t6 = EmptyOperator(
        task_id="t6"
    )

    t7 = EmptyOperator(
        task_id="t7"
    )

    t8 = EmptyOperator(
        task_id="t8"
    )

    t1 >> [t3, t2] >> t4 
    [t4, t7] >> t6 >> t8
    t5 >> t4
