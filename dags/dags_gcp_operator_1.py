from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def get_query_for_yesterday_partition() -> str:
    return """
        SELECT id, player1_id, battle_date as event_date FROM `gcloud-yj.basic.battle` WHERE battle_date = PARSE_DATE('%Y-%m-%d', '2023-07-16')
    """

with DAG(
    dag_id="dags_gcp_operator_1",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    overwrite_partition = BigQueryInsertJobOperator(
        task_id="overwrite_partition_query_only",
        configuration={
            "query": {
                "query": get_query_for_yesterday_partition(),
                "destinationTable": {
                    "projectId": "gcloud-yj",
                    "datasetId": "basic",
                    "tableId": "test_by_partition"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
                "useLegacySql": False
            }
        },
        location="US",
        gcp_conn_id="airflow_bigquery_test",
    )