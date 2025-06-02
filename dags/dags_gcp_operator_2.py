from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def print_bq_result(ti):
    result = ti.xcom_pull(task_ids='get_bq_result')
    print("BigQuery Query Result:")
    for row in result:
        print(row)


with DAG(
    dag_id="dags_gcp_operator",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    # Step 1: BigQuery 쿼리를 결과 테이블에 저장
    export_query_to_gcs = BigQueryInsertJobOperator(
        task_id="export_query_to_gcs",
        configuration={
            "query": {
                "query": """
                    SELECT id, player1_id, event_date FROM `gcloud-yj.basic.test_by_partition`
                    LIMIT 5
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "gcloud-yj",
                    "datasetId": "basic",
                    "tableId": "test_by_partition_temp"
                },
                "writeDisposition": "WRITE_TRUNCATE",
            },"extract": {
                "sourceTable": {
                    "projectId": "gcloud-yj",  
                    "datasetId": "basic",
                    "tableId": "test_by_partition_temp"
                },
                "destinationUris": [
                    "gs://jin_com/airflow/output.csv"  
                ],
                "destinationFormat": "CSV",
                "printHeader": True,
            },
        },
        location="US",
        gcp_conn_id="airflow_bigquery_test",
    )

   