from airflow.models.dag import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id="dags_file_sensor",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    tags=["testtest1"]
) as dag:
    
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id="tvCorona19VaccinestatNew_sensor",
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24,
        mode='reschedule'
    )