from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import bigquery
from datetime import datetime

# ---------------------------
# 1. SQL 생성 함수
# ---------------------------
def generate_insert_sql_chunks():
    total_inserts = 6
    group_size = 2
    results = []
    current_group = []

    for i in range(1, total_inserts + 1):
        table_index = ((i - 1) % 3) + 1
        sql = f"""
        INSERT INTO `basic.table_{table_index}` (id, name, created_at)
        VALUES ({i}, 'name_{i}', CURRENT_TIMESTAMP())
        """
        current_group.append(sql)

        if len(current_group) == group_size:
            results.append(current_group)
            current_group = []

    if current_group:
        results.append(current_group)

    return results  # 예: [['sql1','sql2'], ['sql3','sql4'], ['sql5','sql6']]

# ---------------------------
# 2. SQL 실행 함수
# ---------------------------
def execute_sql_chunk(row):
    # Airflow Connection에서 GCP 인증 정보 가져오기
    hook = GoogleBaseHook(gcp_conn_id="airflow_bigquery_test")
    credentials = hook.get_credentials()
    project_id = hook.project_id

    client = bigquery.Client(credentials=credentials, project=project_id)

    for sql in row:
        job = client.query(sql)
        job.result()  # 완료 대기
        print(f"Executed SQL: {sql}")

# ---------------------------
# DAG 정의
# ---------------------------
with DAG(
    dag_id="bq_dynamic_insert_client",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 시작 Empty Task
    start = EmptyOperator(task_id="start")

    # SQL 생성 Task
    generate_sql_task = PythonOperator(
        task_id="generate_insert_sql_chunks",
        python_callable=generate_insert_sql_chunks
    )

    # SQL 실행 Dynamic Task Mapping
    execute_sql_task = PythonOperator.partial(
        task_id="execute_insert_sql_chunk",
        python_callable=execute_sql_chunk
    ).expand(
        op_kwargs=generate_sql_task.output.map(lambda r: {"row": r})
    )

    # 종료 Empty Task (모든 task 완료 대기)
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> generate_sql_task >> execute_sql_task >> end
