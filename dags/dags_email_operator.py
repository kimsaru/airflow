from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator

import datetime
import pendulum

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'kyj0540@naver.com',
        subject='airflow 성공메일',
        html_content='airflow 작업이 완료되었습니다.'
    )