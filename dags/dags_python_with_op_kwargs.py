import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    regist2_t2 = PythonOperator(
        task_id = 'regist2_t2',
        python_callable=regist2,
        op_args=['hjkim','man','kr','seoul'],
        op_kwargs={'email':'hjkim_sun@naver.com', 'phone':'010'}
    )

    regist2_t2