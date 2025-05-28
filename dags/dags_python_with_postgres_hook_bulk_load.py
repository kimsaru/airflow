import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="dags_python_with_postgres_hook_bulk_load",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    def insrt_postgres(conn_db_postgres_custom, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(conn_db_postgres_custom)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'conn_db_postgres_custom':'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19CountStatus',
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbCorona19CountStatus.csv'}
    )

    insrt_postgres