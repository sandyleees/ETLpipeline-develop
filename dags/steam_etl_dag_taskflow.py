import sys
import os

# DAG 파일이 파싱될 때 /opt/airflow 경로가 없는 경우를 대비
# tasks/ 폴더를 찾을 수 있도록 경로 추가
sys.path.append('/opt/airflow')


from airflow.decorators import dag          # @dag 데코레이터
from datetime import datetime, timedelta

# tasks/ 폴더에서 @task 함수들 import
# 각 파일에 로직이 있고 DAG 파일은 흐름만 정의
from tasks.extract_taskflow import extract
from tasks.transform_taskflow import transform
from tasks.load_taskflow import load

# DAG 기본 설정
default_args = {
    'owner': 'sandy',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='steam_etl_pipeline_taskflow',   # 기존 DAG과 이름 달라야 함
    default_args=default_args,
    description='Steam ETL 파이프라인 (TaskFlow 방식)',
    # schedule_interval='@daily',
    schedule='@daily',          # ← 수정
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def steam_etl_pipeline_taskflow():
    """
    [기존 steam_etl_dag.py와 다른 점]

    기존 BashOperator 방식:
      extract_task = BashOperator(bash_command='python3 tasks/extract.py')
      transform_task = BashOperator(bash_command='python3 tasks/transform.py')
      load_task = BashOperator(bash_command='python3 tasks/load.py')
      extract_task >> transform_task >> load_task
      → 각 Task가 bash 명령어로 별도 프로세스 실행
      → CSV 파일로 데이터 전달

    TaskFlow 방식:
      raw = extract()
      cleaned = transform(raw)
      load(cleaned)
      → 함수 호출 순서가 곧 Task 순서
      → return값이 XCom으로 자동 전달
      → CSV 파일 없이 데이터 전달
    """

    # extract() 실행 → return값이 XCom에 자동 저장
    raw = extract()

    # transform(raw) 실행
    # raw = XCom에서 꺼낸 extract()의 return값
    # → Airflow가 자동으로 XCom에서 꺼내서 인자로 넣어줌
    cleaned = transform(raw)

    # load(cleaned) 실행
    # cleaned = XCom에서 꺼낸 transform()의 return값
    load(cleaned)


# DAG 객체 생성
# @dag 데코레이터 붙은 함수를 호출해야 Airflow가 DAG으로 인식
steam_etl_pipeline_taskflow()