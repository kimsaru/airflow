from datetime import datetime, timedelta
import base64
import json
import time
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

def send_signed_request(**context):
    import cryptography
    print(cryptography.__version__)


default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='test3',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["testtest3"],
    catchup=False
) as dag:
    send_signed_request_task = PythonOperator(
        task_id="send_signed_request_task",
        python_callable=send_signed_request,
        provide_context=True,
    )

    send_signed_request_task
