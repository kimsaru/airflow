from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='dags_gcp_operator_3',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. 임시 테이블에 쿼리 실행
    query_task = BigQueryExecuteQueryOperator(
        task_id='query_task',
        sql="""
            SELECT id, player1_id, event_date
            FROM `gcloud-yj.basic.test_by_partition`
        """,
        use_legacy_sql=False,
        destination_dataset_table='gcloud-yj.basic.test_by_partition_temp1', 
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='airflow_bigquery_test',
        location='US',
    )

    # 2. 쿼리 결과를 GCS로 export
    export_task = BigQueryToGCSOperator(
        task_id='export_task',
        source_project_dataset_table='gcloud-yj.basic.test_by_partition_temp1',
        destination_cloud_storage_uris=['gs://jin_com/airflow/output1.csv'],  
        export_format='CSV',
        field_delimiter=',',
        print_header=True,
        gcp_conn_id='airflow_bigquery_test',
    )

    query_task >> export_task
