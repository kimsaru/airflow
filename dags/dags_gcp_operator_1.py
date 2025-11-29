from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

def print_gcp_credentials():
    hook = GoogleBaseHook(gcp_conn_id="airflow_bigquery_test")
    credentials = hook.get_credentials()
    project_id = hook.project_id

    print("Project ID:", project_id)
    print("Credentials type:", type(credentials))
    # service account info 확인 가능
    if hasattr(credentials, "service_account_email"):
        print("Service account email:", credentials.service_account_email)

with DAG(
    dag_id="dags_gcp_operator_1",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery"],
) as dag:

    test_task = PythonOperator(
        task_id="print_gcp_credentials",
        python_callable=print_gcp_credentials,
    )
