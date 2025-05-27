from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator
from airflow.decorators import dag, task

import datetime
import pendulum

with DAG(
    dag_id="dags_python_email_xcom",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    @task(task_id='something_task')
    def something_logic(**kwargs):
        from random import choice
        return choice(['Success','Fail'])
    
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'kyj0540@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                    {{ ti.xcom_pull(task_ids="something_task") }} 했습니다 <br>'
    )

    something_logic() >> send_email_task
