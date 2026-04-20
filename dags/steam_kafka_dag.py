import sys
from airflow.decorators import dag, task
from datetime import datetime, timedelta

# DAG 파일이 파싱될 때 /opt/airflow 경로가 없는 경우를 대비
# tasks/ 폴더를 찾을 수 있도록 경로 추가
sys.path.append('/opt/airflow')

default_args = {
    'owner': 'sandy',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='steam_kafka_pipeline',
    default_args=default_args,
    description='Steam 데이터를 Kafka로 전송하는 파이프라인',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def steam_kafka_pipeline():

    @task()
    def kafka_produce():
        # tasks/kafka_producer.py의 main() 함수 직접 호출
        # BashOperator로 python3 실행하는 것과 달리
        # 같은 Python 프로세스 안에서 함수 호출 → import로 연결
        from tasks.kafka_producer import main
        main()

    kafka_produce()

steam_kafka_pipeline()