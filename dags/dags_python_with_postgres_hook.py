import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_postgres_hook",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    tags=["testtest1"],
    catchup=False
) as dag:
    
    def insrt_postgres(conn_db_postgres_custom, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing

        postgres_hook = PostgresHook(conn_db_postgres_custom)

        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insrt 수행'
                sql = 'insert into py_opr_drct_insrt values(%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id,task_id,run_id,msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'conn_db_postgres_custom':'conn-db-postgres-custom'}
    )

    insrt_postgres