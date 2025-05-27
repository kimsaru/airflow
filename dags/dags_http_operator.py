from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task

import datetime
import pendulum

with DAG(
    dag_id="dags_http_operator",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    tags=["testtest1"]
) as dag:
    
    tb_cycle_station_info = HttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{ var.value.apikey_openapi.seoul.go.kr}}/json/tbCycleStationInfo/1/10',
        method='GET',
        headers={
            'Content-Type':'application/json',
            'charset':'utf-8',
            'Accept': '*/*'
        }
    )

    @task(task_id="python_2")
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_cycle_station_info >> python_2()