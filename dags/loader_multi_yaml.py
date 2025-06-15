import yaml
import os
from airflow import DAG
from airflow.utils.module_loading import import_string
from datetime import datetime

def load_yaml_dag(yaml_path):
    with open(yaml_path) as f:
        config = yaml.safe_load(f)

    dag_config = config['dag']

    def parse_start_date(val):
        # val이 이미 datetime 또는 date 타입이면 datetime으로 변환 후 반환
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime(val.year, val.month, val.day)
        # 문자열이면 fromisoformat 사용
        if isinstance(val, str):
            return datetime.fromisoformat(val)
        raise ValueError(f"Cannot parse start_date: {val}")

    # 기존 코드에서 start_date 부분만 변경
    start_date = parse_start_date(dag_config['start_date'])
    dag = DAG(
        dag_id=dag_config['dag_id'],
        start_date=start_date,
        schedule_interval=dag_config.get('schedule_interval'),
        catchup=dag_config.get('catchup', False),
    )

    tasks = {}
    for task_cfg in config['tasks']:
        operator_class = import_string(task_cfg['operator'])
        params = {k: v for k, v in task_cfg.items() if k not in ['task_id', 'dependencies', 'operator']}
        task = operator_class(task_id=task_cfg['task_id'], dag=dag, **params)
        tasks[task.task_id] = task

    for task_cfg in config['tasks']:
        for dep in task_cfg.get('dependencies', []):
            tasks[dep] >> tasks[task_cfg['task_id']]

    return dag

yaml_dir = os.path.join(os.path.dirname(__file__), "yaml_dags")
for filename in os.listdir(yaml_dir):
    if filename.endswith(".yaml"):
        dag = load_yaml_dag(os.path.join(yaml_dir, filename))
        globals()[dag.dag_id] = dag