from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from google.cloud import bigquery
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# -----------------------------
# 함수 정의
# -----------------------------

def generate_insert_sql_chunks():
    """
    200개 이상 SQL 생성 → chunk_size=100 으로 나누어
    Dynamic Task Mapping에서 task가 2개 생성되도록 함
    """
    countries = [f"country_{i}" for i in range(10)]  # 20개 나라
    tests = [f"test_{i}" for i in range(11)]        # 10개 테스트

    # 10 × 11 = 200개 SQL 생성됨
    all_sql = []

    for c in countries:
        for t in tests:
            sql = f"""
            INSERT INTO `basic.my_table` 
            (country, test_name, created_at, is_exist)
            VALUES ('{c}', '{t}', CURRENT_TIMESTAMP(), 'n')
            """
            all_sql.append(sql)

    # 100개씩 chunk
    chunk_size = 50
    results = [all_sql[i:i + chunk_size] for i in range(0, len(all_sql), chunk_size)]

    print(f"총 SQL 개수: {len(all_sql)}")
    print(f"chunk 개수: {len(results)}")
    print(f"첫 번째 chunk 크기: {len(results[0])}")
    print(f"두 번째 chunk 크기: {len(results[1])}")

    return results

def execute_sql_chunk(row, gcp_conn_id="airflow_bigquery_test"):
    """
    각 chunk의 SQL 리스트 실행
    """
    # Airflow Connection에서 Credential 가져오기
    hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
    credentials = hook.get_credentials()
    project_id = hook.project_id

    # BigQuery Client 생성
    client = bigquery.Client(credentials=credentials, project=project_id)

    for sql in row:
        query_job = client.query(sql)
        query_job.result()  # 실행 완료까지 대기
        print(f"sql: {sql}")

# -----------------------------
# DAG 정의
# -----------------------------

with DAG(
    dag_id="bq_dynamic_task_mapping_chunks",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Start
    start = EmptyOperator(task_id="start")

    # SQL 생성 Task
    generate_sql_task = PythonOperator(
        task_id="generate_sql_chunks",
        python_callable=generate_insert_sql_chunks
    )

    # Dynamic Task Mapping으로 SQL chunk 실행
    execute_chunks = PythonOperator.partial(
        task_id="execute_sql_chunk",
        python_callable=execute_sql_chunk
    ).expand(
        op_kwargs=generate_sql_task.output.map(lambda r: {"row": r})
    )

    # End
    end = EmptyOperator(task_id="end")

    # Task Flow
    start >> generate_sql_task >> execute_chunks >> end