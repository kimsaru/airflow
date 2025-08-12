from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import os
import time
import threading

def process_df(part_df, idx):
    pid = os.getpid()
    tid = threading.get_ident()  # 스레드 ID
    print(f"[스레드 {idx}] PID: {pid}, TID: {tid} - 처리 시작 - 행 수: {len(part_df)}")
    # 처리 작업 예시 (시간 지연 추가)
    time.sleep(2)
    
    print(f"[스레드 {idx}] PID: {pid} - 처리 완료")
    return len(part_df)

def parallel_dataframe_processing():
    df = pd.DataFrame({
        'ID': range(1000),
        'Value': [x * 2 for x in range(1000)]
    })

    num_parts = 5
    indices = np.array_split(df.index, num_parts)
    df_parts = [df.loc[i].reset_index(drop=True) for i in indices]

    with ThreadPoolExecutor(max_workers=num_parts) as executor:
        futures = []
        for idx, part in enumerate(df_parts):
            futures.append(executor.submit(process_df, part, idx))

        for i, future in enumerate(futures):
            done = future.done()
            print(f"[메인] 스레드 {i} 완료 여부: {done}")
            result = future.result()
            print(f"[메인] 스레드 {i} 결과: {result}")

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG('dag_threadpool',
         schedule_interval=None,
         catchup=False,
         default_args=default_args) as dag:

    task = PythonOperator(
        task_id='run_parallel_dataframe_processing',
        python_callable=parallel_dataframe_processing
    )
