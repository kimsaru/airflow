from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id="dags_seoul_api_corona",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    tags=["testtest1"]
) as dag:
    
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus', 
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbCorona19CountStatus.csv'
    )

    tv_Corona19_Vaccine_statNew = SeoulApiToCsvOperator(
        task_id='tv_Corona19_Vaccine_statNew',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tv_Corona19_Vaccine_statNew