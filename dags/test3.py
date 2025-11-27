from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime

# ------------------------------
# batch util
# ------------------------------
def chunk_list(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]


# ------------------------------
# DAG
# ------------------------------
@dag(
    dag_id="test3",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"]
)
def parallel_query_dag():

    # A task (optional)
    @task
    def start():
        return "start"

    # B task: 쿼리 배열 가져오기
    @task
    def get_queries():
        # 실제론 DB나 API에서 가져오겠죠
        # 아래는 예시:
        queries = [f"SELECT {i}" for i in range(37)]  # 37개 쿼리 예시
        return queries

    # B 이후: 10개씩 묶어서 병렬 실행할 task
    @task
    def run_query_batch(query_batch):
        # 여기서 실제 SQL 실행
        results = []
        for q in query_batch:
            # 예: DB 커넥션 호출
            print(f"Running query: {q}")
            # run_sql(q) 같은 함수 호출 가능
            results.append(f"OK: {q}")

        return results

    # C, D, E 다음 작업들
    @task
    def c_task():
        print("C 실행")

    @task
    def d_task():
        print("D 실행")

    @task
    def e_task():
        print("E 실행")


    # ------------------------------
    # DAG 실행 흐름
    # ------------------------------
    start_task = start()

    queries = get_queries()

    # Airflow 방식으로 변환 task 추가
    @task
    def make_batches(q_list):
        return chunk_list(q_list, 10)

    batches = make_batches(queries)

    # batch를 dynamic mapping → run_query_batch 여러 개 생성
    run_batches = run_query_batch.expand(query_batch=batches)

    # 이후 직렬 task들
    c = c_task()
    d = d_task()
    e = e_task()

    start_task >> queries >> batches >> run_batches >> c >> d >> e


parallel_query_dag()
