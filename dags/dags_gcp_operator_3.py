from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dags_gcp_operator_3",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    run_query = BigQueryInsertJobOperator(
        task_id="run_query",
        configuration={
            "load": {
                "sourceUris": ["gs://jin_com/airflow/output.csv"],
                "destinationTable": {
                    "projectId": "gcloud-yj",
                    "datasetId": "basic",
                    "tableId": "test_by_partition_temp2"
                },
                "sourceFormat": "CSV",
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
                "skipLeadingRows": 1,
                "timePartitioning": {
                    "type": "DAY",
                    "field": "event_date",  
                },
            }
        },
        location="US",
        gcp_conn_id="airflow_bigquery_test",
    )

