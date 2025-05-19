import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp, regist

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist = PythonOperator(
        task_id = 'regist',
        python_callable = regist,
        op_args = ['kim','man','kr','seoul']
    )

    regist