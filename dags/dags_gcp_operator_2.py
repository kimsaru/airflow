from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def check_word_count(**context):
    hook = BigQueryHook(gcp_conn_id="airflow_bigquery_test", location="US")
    sql = """
        SELECT id, player1_id, event_date
        FROM `gcloud-yj.basic.test_by_partition_temp`
    """
    df = hook.get_pandas_df(sql=sql)
    print(f"{df}")

with DAG(
    dag_id="bq_query_export_check",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_query = BigQueryInsertJobOperator(
        task_id="run_query",
        configuration={
            "query": {
                "query": """
                    SELECT id, player1_id, event_date FROM `gcloud-yj.basic.test_by_partition`
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "gcloud-yj",
                    "datasetId": "basic",
                    "tableId": "test_by_partition_temp1"
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
        gcp_conn_id="airflow_bigquery_test",
    )

    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_to_gcs",
        source_project_dataset_table="gcloud-yj.basic.test_by_partition_temp1",
        destination_cloud_storage_uris=["gs://jin_com/airflow/output.csv"],
        export_format="CSV",
        print_header=True,
        gcp_conn_id="airflow_bigquery_test",
    )

    check_condition = PythonOperator(
        task_id="check_condition",
        python_callable=check_word_count,
    )

    run_query >> export_to_gcs >> check_condition
