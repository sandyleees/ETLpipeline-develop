from airflow.decorators import task  # @task 데코레이터
# from airflow.hooks.base import BaseHook  # Airflow UI Connection 정보 꺼내오는 도구 -- phase4부터 불필요(connection 안씀)
import pandas as pd
# import psycopg2                      # PostgreSQL 연결 라이브러리 -- phase4부터 불필요 (postgreSQL 안씀)
                                     # psycopg2 = Python에서 PostgreSQL 접속할 때 쓰는 표준 라이브러리
# Python 코드  ──psycopg2──►  PostgreSQL (steam_db)
# psycopg2 = Python과 PostgreSQL 사이의 통역사
#            Python 코드를 PostgreSQL이 이해하는 방식으로 변환해줌

import boto3                          # AWS S3 접근하기 위해 Python → AWS 통신을 대신해주는 라이브러리
import os                             # .env -> docker compose 자동 읽기 -> 컨테이너 내 환경변수로 주입됨 : 운영체제 환경변수 읽기 
                                    # => 컨테이너에 위에서 py 파일 돌아가므로 컨테이너 환경변수 읽음 (.env 파일 읽는게 아님. docker compose가 .env -> 환경변수로 바꿔준거 읽는거임)
                                    # 코드에 키를 직접 쓰지 않기 위한 안전장치 
from io import StringIO               # DataFrame → 문자열 변환 (파일 없이 S3 업로드) # 메모리 안에서 파일처럼 쓸 수 있는 객체
                                    # 로컬에 CSV 파일 저장 없이
                                    # 바로 S3에 업로드하기 위해 필요
                                    # io = Input/Output 의 약자
from datetime import datetime         # 날짜별 파일 이름 생성용


@task
def load(data):  # ← transform()의 return값(딕셔너리 리스트)을 인자로 받음 # XCom을 통해 자동으로 전달됨
    """
    transform()에서 받은 정제 데이터를 S3에 업로드하는 함수

    [기존 load.py와 다른 점]
    기존: XCom 데이터 → steam_db에 INSERT
    새것: XCom 데이터 → S3 버킷에 CSV 업로드
    """

    # DataFrame으로 변환
    # transform()이 딕셔너리 리스트로 넘겨줬으니까 다시 DataFrame으로
    df = pd.DataFrame(data)

    # S3 클라이언트 생성
    # boto3가 .env의 환경변수를 자동으로 읽어서 인증
    # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY 를 os.environ에서 찾음
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION')
    )
    # client = "S3랑 통신할 수 있는 객체(인스턴스)" <- 클래스 __init__ 설정값 작성해서 만들어짐
    #
    # boto3.client('s3') = S3 전용 통신 도구 생성
    #
    # os.environ.get('AWS_ACCESS_KEY_ID')
    #   → docker-compose가 .env를 읽어서
    #     컨테이너 환경변수로 주입한 값을 꺼내옴 (.env 읽는게 아님)
    #   → 코드에 키를 직접 안 써도 되는 이유
    #
    # region_name = 어느 리전의 S3인지 명시
    #   → 서울 리전: ap-northeast-2


    # 버킷 이름 가져오기
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    # 코드에 직접 쓰지 않고 환경변수로 관리
    # → 버킷 이름 바뀌어도 .env만 수정하면 됨


    # 날짜별 파일 이름 생성 # 오늘 날짜 기반 파일 이름 생성
    # 예: deals_20260414.csv
    today = datetime.now().strftime('%Y%m%d')
    file_name = f'deals_{today}.csv'
    # datetime.now() = 현재 시각
    # .strftime('%Y%m%d') = 날짜 형식 지정
    #   %Y = 4자리 연도 (2026)
    #   %m = 2자리 월   (04)
    #   %d = 2자리 일   (14)
    # → '20260414'
    #
    # f'deals_{today}.csv' = 'deals_20260414.csv'
    # DAG 매일 실행 시 날짜별로 파일이 쌓임:
    #   deals_20260414.csv
    #   deals_20260415.csv
    #   deals_20260416.csv


    # DataFrame → CSV 문자열로 변환
    # StringIO = 메모리 안에서 파일처럼 쓸 수 있는 객체
    # 로컬에 파일 저장 없이 바로 S3에 업로드 가능
    # 1. 빈 StringIO 객체 생성
    # 메모리 안에 빈 공간 만들기
    csv_buffer = StringIO()
    # 내부 상태: ""  (비어있음)
    # 커서 위치: 0
    #  ↓
    # [        ] ← 빈 메모리 공간
    #  ↑
    #  커서


    # 2. DataFrame을 CSV 형태로 StringIO에 씀
    df.to_csv(csv_buffer, index=False)  # index=False = 행 번호 제외
    # 내부 상태:
    # "title,salePrice,normalPrice\n
    #  게임A,9.99,19.99\n
    #  게임B,4.99,9.99\n"
    #
    #  [title,salePrice...\n게임A,9.99...\n게임B...]
    #                                               ↑
    #                                          커서가 맨 끝으로 이동

    # StringIO() = 메모리 안의 빈 파일 같은 것
    #   실제 파일처럼 읽고 쓸 수 있지만
    #   디스크에 저장되지 않음 (메모리에만 존재)
    #
    # df.to_csv(csv_buffer)
    #   = DataFrame을 CSV 형태로 csv_buffer에 씀
    #   = 로컬 파일 저장 없이 메모리에 CSV 생성
    #
    # index=False
    #   = 행 번호(0,1,2...) 제외하고 저장
    #   없으면 CSV 첫 번째 열에 0,1,2... 숫자가 생김

    print(f"S3 업로드 시작: {bucket_name}/{file_name} ({len(df)}개)")

    # S3에 파일 업로드
    s3.put_object(
        Bucket=bucket_name,       # 어느 버킷에
        Key=file_name,            # 파일 이름 (경로 포함 가능: 'data/deals_20260414.csv')
        # 3. StringIO에 담긴 내용 꺼내기
        Body=csv_buffer.getvalue(), # 업로드할 내용 (CSV 문자열) 
        # getvalue() = 커서 위치 상관없이
        #              전체 내용을 문자열로 반환
        # → "title,salePrice,normalPrice\n게임A,9.99..."
        ContentType='text/csv'    # 파일 형식 명시
    )
    # Key = S3 안에서의 파일 경로/이름
    #   'deals_20260414.csv'
    #   'data/deals_20260414.csv' 처럼 폴더 구조도 가능
    #
    # Body = 업로드할 실제 내용
    #   csv_buffer.getvalue() = StringIO에 담긴 CSV 문자열 꺼내기
    #
    # ContentType = 파일 형식 명시
    #   'text/csv' = CSV 파일임을 S3에 알려줌
    #   없어도 동작하지만 명시하는 게 좋은 습관

    print(f"S3 업로드 완료: s3://{bucket_name}/{file_name}")