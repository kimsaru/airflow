from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# def print_bq_result(ti):
#     result = ti.xcom_pull(task_ids='get_bq_result')
#     print("BigQuery Query Result:")
#     for row in result:
#         print(row)


with DAG(
    dag_id="dags_gcp_operator",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    # Step 1: BigQuery 쿼리를 결과 테이블에 저장
    run_query = BigQueryInsertJobOperator(
        task_id="run_bigquery_select",
        configuration={
            "query": {
                "query": """
                    SELECT id, kor_name, eng_name FROM `gcloud-yj.basic.pokemon`
                    LIMIT 5
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "gcloud-yj",
                    "datasetId": "basic",
                    "tableId": "pokemon_1"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        location="US",
        gcp_conn_id="airflow_bigquery_test",
    )

    # Step 2: 쿼리 결과 가져오기
    # get_result = BigQueryGetDataOperator(
    #     task_id="get_bq_result",
    #     dataset_id="basic",
    #     table_id="pokemon",
    #     max_results=5,
    #     gcp_conn_id="airflow_bigquery_test",
    # )

    # Step 3: 결과 로그에 출력
    # log_result = PythonOperator(
    #     task_id="log_bq_result",
    #     python_callable=print_bq_result,
    # )

    # run_query >> 
    # get_result >> log_result
