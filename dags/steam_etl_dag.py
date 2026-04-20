from airflow import DAG # Airflow에서 DAG 클래스 가져오기
        # 컨테이너 안 Airflow 라이브러리에서 가져오는 거예요. 로컬엔 Airflow 설치 안 해도 돼요.
                        # DAG = 파이프라인 전체를 정의하는 틀
from airflow.operators.bash import BashOperator
# BashOperator = bash 명령어를 Task로 실행하는 도구
# 우리 tasks/*.py 파일들을 python3 명령어로 실행할 거예요
# bash_command에 적은 명령어를 컨테이너 안 bash 쉘로 실행해요
# 즉 "python3 tasks/extract.py" 를
# 컨테이너 안 bash 쉘에서 실행하는 거예요

from datetime import datetime, timedelta # 날짜/시간 관련 라이브러리

# DAG 기본 설정 - 딕셔너리형이라 가독성 안좋아져서 & 재사용 가능해서 위에 따로 정의
# with DAG (...) <- 여기 들어갈거임 
default_args = {
    'owner': 'sandy',               # DAG 소유자 이름 - Airflow UI에서 표시됨
                                    # 여러 사람이 쓸 때 누가 만든 DAG인지 구분용
    'retries': 1,               # Task 실패시 재시도 횟수 # 1 = 한 번 더 시도
    'retry_delay': timedelta(minutes=1)  # 재시도 간격 # 실패하면 1분 기다렸다가 재시도
}

# 크게 3부분으로 구성돼요 (DAG : 파이프라인 순서를 Airflow에 알려주는 파일)
# 1. DAG 정의 (이름, 스케줄, 시작일 등)
# DAG 정의 <- with 문법 = "이 블록 안에서만 이걸 써" (Python의 컨텍스트 매니저 문법)
with DAG(
    dag_id='steam_etl_pipeline',    # DAG 이름 (Airflow UI에서 보이는 이름) - 중복되면 안 됨
    default_args=default_args,      # 위에서 정의한 기본 설정 적용 --- 따로 빼둔거 여기에 쓸 예정이었음
    description='Steam ETL 파이프라인',  # DAG 설명 - UI에서 표시됨
    # schedule_interval='@daily',     # 실행 주기 # 매일 자동 실행 # @daily = 매일 자정 자동 실행
                                    # 다른 옵션들:
                                    # '@hourly'  = 매시간
                                    # '@weekly'  = 매주
                                    # '0 9 * * *' = 매일 오전 9시 (Cron 표현식)
    schedule='@daily',          # ← 수정
    start_date=datetime(2024, 1, 1),    # DAG 시작일 # "2024년 1월 1일부터 스케줄 시작해"
    # 2024년 1월 1일은 특별한 이유 없고 그냥 과거 날짜면 아무 날짜나 괜찮아요.
    # 중요한 건 catchup=False 랑 같이 쓴다는 거 → 과거는 무시하고 지금부터 실행
    catchup=False,           # 과거 날짜 소급(과거에까지 거슬러 올라가서 미치게 함) 실행 안 함
                                    # True면 start_date부터 오늘까지 밀린 거 다 실행
                                    #    → Airflow가 2024년 1월 1일부터 오늘까지
                                    #   매일 한 번씩 DAG 실행 시도
                                    #   = 약 820번 실행 ❌ (우리가 원하는 게 아님)
                                    # False면 지금부터만 실행
                                    # → 우리는 False (과거 데이터 필요 없으니까) → 오늘부터만 실행 ✅
) as dag:
    # 블록 안에서 Task 정의하면
    # 자동으로 DAG에 등록됨
    # 2. Task 정의 (각 단계)

    # Task 1: Extract
    # BashOperator로 tasks/extract.py 실행
    extract_task = BashOperator(
        task_id='extract',          # Task 고유 이름 (Airflow UI에서 보이는 이름)
        bash_command='cd /opt/airflow && python3 tasks/extract.py',
        # cd로 먼저 /opt/airflow (작업 디렉토리)로 이동 후 실행
        # (BashOperator가 실행할 때 작업 디렉토리가 /opt/airflow/ 가 아닐 수 있어서)
        # 컨테이너 안 경로로 실행 (/opt/airflow/tasks/ = 우리 tasks/ 폴더)
        # 컨테이너 안 bash 쉘에서 실행할 명령어
        # /opt/airflow/tasks/ = 우리 로컬 tasks/ 폴더와 연결된 경로
        # (Volume으로 연결해뒀으니까 같은 파일)
        
    )

    # Task 2: Transform
    transform_task = BashOperator(
        task_id='transform',
        bash_command='cd /opt/airflow && python3 tasks/transform.py',
    )

    # Task 3: Load
    load_task = BashOperator(
        task_id='load',
        bash_command='cd /opt/airflow && python3 tasks/load.py',
    )
   
   
    # 3. Task 순서 정의
    # Task 실행 순서 정의
    # >> 연산자 = "다음에 실행해"
    extract_task >> transform_task >> load_task
    # extract 완료 → transform 실행
    # transform 완료 → load 실행
    # 하나라도 실패하면 다음 Task 실행 안 함