import yaml
import os
from airflow import DAG
from airflow.utils.module_loading import import_string
from datetime import datetime

def load_yaml_dag(yaml_path):
    with open(yaml_path) as f:
        config = yaml.safe_load(f)

    dag_config = config['dag']
    dag = DAG(
        dag_id=dag_config['dag_id'],
        start_date=datetime.fromisoformat(dag_config['start_date']),
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