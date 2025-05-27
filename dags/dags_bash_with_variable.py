from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

import datetime
import pendulum

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2025, 5, 26, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testtest1"],
    # params={"example_key": "example_value"}, 모든 task들에게 공통적으로 넘겨줄 마라메타타
) as dag:
    
    var_value = Variable.get("sample_key")

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command=f"echo variable: {var_value}"
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo variable: {{ var.value.sample_key}}"
    )

    bash_t1 >> bash_t2