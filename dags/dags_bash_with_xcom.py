from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import datetime
import pendulum

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo START && "
                     "echo XCOM_PUSHED"
                     "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} && "
                     "echo COMPLETE"
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed')}}",
             'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push')}}"
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False
    )


    bash_push >> bash_pull