from datetime import datetime, timedelta
import base64
import json
import time
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def send_signed_request(**context):
    import cryptography
    print(cryptography.__version__)


with DAG(
    dag_id="test3",
    schedule_interval=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["testtest1"],
) as dag:

    send_signed_request_task = PythonOperator(
        task_id="send_signed_request_task",
        python_callable=send_signed_request,
        provide_context=True,
    )

    send_signed_request_task
