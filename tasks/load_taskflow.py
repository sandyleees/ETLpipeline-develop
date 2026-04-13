from airflow.decorators import task  # @task 데코레이터
from airflow.hooks.base import BaseHook  # Airflow UI Connection 정보 꺼내오는 도구
import pandas as pd
import psycopg2                      # PostgreSQL 연결 라이브러리
                                     # psycopg2 = Python에서 PostgreSQL 접속할 때 쓰는 표준 라이브러리

# Python 코드  ──psycopg2──►  PostgreSQL (steam_db)

# psycopg2 = Python과 PostgreSQL 사이의 통역사
#            Python 코드를 PostgreSQL이 이해하는 방식으로 변환해줌

@task
def load(data):  # ← transform()의 return값(딕셔너리 리스트)을 인자로 받음
    """
    transform()에서 받은 정제 데이터를 steam_db에 저장하는 함수

    [기존 load.py와 다른 점]
    기존 : data/clean.csv 파일을 읽어서 → data/steam_deals.csv로 저장
    새것 : transform()의 return값을 인자로 받아서 → steam_db에 저장
           CSV 파일 없이 DB에 직접 적재
    """

    # DataFrame으로 변환
    # transform()이 딕셔너리 리스트로 넘겨줬으니까 다시 DataFrame으로
    df = pd.DataFrame(data)


    # Airflow UI Admin → Connections 에 등록한 정보 가져오기
    # 'steam_db_conn' = 우리가 UI에서 등록한 Connection ID
    conn_info = BaseHook.get_connection('steam_db_conn')

    # conn_info 안에 이런 정보가 들어있어요:
    # conn_info.host     = "postgres"      (컨테이너 이름)
    # conn_info.schema   = "steam_db"      (Database 필드)
    # conn_info.login    = "postgresql"    (Login 필드)
    # conn_info.password = "postgresql"    (Password 필드)
    # conn_info.port     = 5432            (Port 필드)


    # PostgreSQL 연결 설정
    # conn = 연결 그 자체 # conn = psycopg2.connect(...)
    # 마치 PostgreSQL 건물에 들어가는 문을 여는 것  
    # Airflow Connection에 등록한 정보로 접속
    # (Step 6에서 Airflow UI에 등록할 거예요 -- 아래는 하드코딩의 case)
    # conn = psycopg2.connect(
    #     host="postgres",       # docker-compose.yml의 postgres 컨테이너 이름
    #                            # 컨테이너끼리는 컨테이너 이름이 곧 주소예요
    #     database="steam_db",   # 접속할 DB 이름
    #     user="postgresql",        # .env의 POSTGRES_USER
    #     password="postgresql"     # .env의 POSTGRES_PASSWORD
    #                            # ★ 나중에 .env에서 읽어오도록 개선할 거예요
    # )
    conn = psycopg2.connect(
        host=conn_info.host,
        database=conn_info.schema,   # Airflow Connection에서 Database = schema
        user=conn_info.login,
        password=conn_info.password,
        port=conn_info.port
    )


    # cursor = DB에 SQL 명령을 보내는 도구
    # 마치 DB와 대화하는 창구 역할
    cursor = conn.cursor()
    # cursor = 실제로 SQL을 실행하는 도구
    # 마치 건물 안에서 직원한테 요청하는 창구


    # ★ dag 여러 번 실행 시 data 중복 저장 방지: INSERT 전에 기존 데이터 전부 삭제
    # DAG을 여러 번 실행해도 항상 최신 데이터 20개만 유지됨
    # TRUNCATE = DELETE보다 빠름 (행 하나씩 지우지 않고 테이블 통째로 초기화)
    cursor.execute("TRUNCATE TABLE steam_deals RESTART IDENTITY")
    # RESTART IDENTITY = id(SERIAL) 번호도 1부터 다시 시작 (PRIMARY KEY)
    # 없으면 id가 21, 22, 23... 으로 계속 증가


    print(f"steam_db 적재 시작: {len(df)}개")

    # 행 하나씩 INSERT # DataFrame을 한 행씩 꺼내주는 pandas 메소드
    for _, row in df.iterrows():
    #   ↑
    #   _ = 행 번호 (0, 1, 2 ...)
    #       쓸 일 없으니까 _ 로 무시
    #
    #        row = 한 행의 데이터
    #              row['title'], row['salePrice'] 이런 식으로 접근

    # iterrows()는 튜플을 하나씩 yield하는 제너레이터예요
    # 각 튜플은 이렇게 생겼어요:
    # (인덱스번호, Series객체)
    # iterrows()가 매 루프마다 이걸 반환해요:
    # _ = 인덱스 번호 (0, 1, 2 ...)
    # row = Series 객체 (한 행의 데이터)

    # 예시: df가 이렇게 생겼으면
    # title        salePrice
    # "게임A"      9.99
    # "게임B"      4.99

    #   (0, Series({'title': '배트맨',    'salePrice': 9.99})),
    #   (1, Series({'title': '스파이더맨', 'salePrice': 4.99})),
    #  ↑                ↑
    #  인덱스번호        한 행의 데이터 (Series)

    # 첫 번째 루프: row['title'] = "게임A", row['salePrice'] = 9.99
    # 두 번째 루프: row['title'] = "게임B", row['salePrice'] = 4.99
        # 창구에 SQL 명령 전달 # cursor.execute("SELECT ...")
        cursor.execute("""
            INSERT INTO steam_deals (
                title,
                sale_price,
                normal_price,
                savings,
                metacritic_score,
                steam_rating,
                deal_rating,
                release_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row['title'],
            row['salePrice'],
            row['normalPrice'],
            row['savings'],
            row['metacriticScore'],
            row['steamRatingText'],
            row['dealRating'],
            row['releaseDate']
        ))
        # %s = SQL 인젝션 방지를 위한 플레이스홀더
        # 값을 직접 문자열로 넣지 않고 튜플로 따로 전달하는 방식

    # 변경사항 확정 (commit하지 않으면 DB에 반영 안 됨)
    # "지금까지 한 작업 확정해줘"
    # commit 안 하면 DB에 반영 안 됨!
    # 마치 문서 작성하고 저장버튼 안 누른 것
    # 트랜잭션 ACID에서 A(원자성) 과 D(지속성) 을 보장
    conn.commit()
    # TRUNCATE + INSERT 전부 하나의 트랜잭션으로 묶임
    # → commit 전에 에러나면 TRUNCATE도 rollback
    # → 데이터가 통째로 날아가는 일 없음 (ACID 원자성)

    print(f"steam_db 적재 완료: {len(df)}개")

    # 연결 종료 # 창구 닫고, 건물 문 닫고 나오기
    cursor.close()
    conn.close()