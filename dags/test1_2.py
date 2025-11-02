from datetime import datetime, timedelta
import base64
import json
import time
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def send_signed_request(**context):
    # ------------------------------
    # 1️⃣ Airflow Variable & Connection에서 정보 로드
    # ------------------------------
    private_key_pem = Variable.get("test_key")
    key_id = Variable.get("test_key_id")


    print(f'private_key_pem {private_key_pem}')
    print(f'key_id {key_id}')
    

    # HTTP Connection 정보 (host, port 등)
    conn = BaseHook.get_connection("api_server_conn")
    host = f"{conn.host}:{conn.port}" if conn.port else conn.host
    base_path = json.loads(conn.extra or "{}").get("base_path", "")

    print(f'conn {conn}')
    print(f'conn.host {conn.host}')
    print(f'conn.port {conn.port}')

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None,
    )

    # ------------------------------
    # 2️⃣ 요청 데이터 설정
    # ------------------------------
    method = "POST"
    # url_path = f"{base_path}/users"
    url_path = "/api/users"
    unix_timestamp = str(int(time.time()))

    data = {
        "id": 99,
        "name": "SecureClient",
        "age": 42
    }
    body_json = json.dumps(data)
    body_bytes = body_json.encode("utf-8")

    print(f'body_json {body_json} {type(body_json)}')
    print(f'body_bytes {type(body_bytes)}')

    # ------------------------------
    # 3️⃣ Digest 헤더 생성
    # ------------------------------
    digest = hashes.Hash(hashes.SHA256())
    digest.update(body_bytes)
    body_hash = digest.finalize()
    digest_header = "SHA-256=" + base64.b64encode(body_hash).decode("utf-8")
    print("Digest:", digest_header)

    # ------------------------------
    # 4️⃣ 서명 문자열 구성
    # ------------------------------
    sign_lines = [
        f"(request-target): {method.lower()} {url_path}",
        f"host: {host}",
        f"date: {unix_timestamp}",
        f"digest: {digest_header}"
    ]
    sign_string = "\n".join(sign_lines).encode("utf-8")
    print(f"sign_string: {sign_string}")


    signature = private_key.sign(
        sign_string,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    signature_b64 = base64.b64encode(signature).decode("utf-8")
    print(f"signature_b64: {signature_b64}")

    # -----------------------------
    # 6. Authorization 헤더
    # -----------------------------
    auth_header = (
        f'Signature keyId="{key_id}",'
        f'algorithm="rsa-sha256",'
        f'headers="(request-target) host date digest",'
        f'signature="{signature_b64}"'
    )

    # -----------------------------
    # 7. 요청 헤더
    # -----------------------------
    headers = {
        "Content-Type": "application/json",
        "Date": unix_timestamp,
        "Host": host,
        "Digest": digest_header,
        "Authorization": auth_header
    }

    # ------------------------------
    # 7️⃣ 요청 전송
    # ------------------------------
    url = f"http://{host}{url_path}"
    print(f"🚀 Sending signed request to: {url}")

    response = requests.post(url, headers=headers, data=body_json)

    print("✅ Status:", response.status_code)
    print("✅ Body:", response.text)

    if response.status_code >= 400:
        raise Exception(f"API call failed: {response.status_code} {response.text}")


with DAG(
    dag_id="test2",
    schedule_interval=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["testtest3"],
) as dag:

    send_signed_request_task = PythonOperator(
        task_id="send_signed_request_task",
        python_callable=send_signed_request,
        provide_context=True,
    )

    send_signed_request_task
