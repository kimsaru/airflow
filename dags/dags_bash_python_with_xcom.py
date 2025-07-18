from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

import datetime
import pendulum

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    @task(task_id="python_push")
    def python_push_xcom():
        result_dict = {'status':'Good','data':[1,2,3],'options_cnt':100}
        return result_dict
    
    bash_pull = BashOperator(
        task_id="bash_pull",
        env={
            'STATUS' : '{{ ti.xcom_pull(task_ids="python_push")["status"] }}',
            'DATA' : '{{ ti.xcom_pull(task_ids="python_push")["data"] }}',
            'OPTIONS_CNT' : '{{ ti.xcom_pull(task_ids="python_push")["options_cnt"] }}'
        },
        bash_command="echo $STATUS && echo $DATA && echo $OPTIONS_CNT"
    )

    python_push_xcom() >> bash_pull

    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo PUSH_START"
                     "{{ ti.xcom_push(key='bash_pushed',value=200)}} && "
                     "echo PUSH_COMPLETE"
    )

    @task(task_id="python_pull")
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key='bash_pushed')
        return_value = ti.xcom_pull(task_ids='bash_push')
        print('status_value: ' + str(status_value))
        print('return_value: ' + return_value)
    
    bash_push >> python_pull_xcom()