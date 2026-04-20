import json
import time
from kafka import KafkaConsumer
# KafkaProducer의 반대 역할
# Kafka 토픽에서 메시지를 읽어오는 클라이언트
from kafka.errors import NoBrokersAvailable

'''
def create_consumer():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                'steam-deals',
                # 구독할 토픽 이름
                # Producer가 메시지를 보낸 바로 그 토픽

                bootstrap_servers='kafka:9092',
                # 브로커 주소 (컨테이너 이름으로 접속)
                # Producer와 동일한 브로커에 접속

                auto_offset_reset='earliest',
                # Consumer가 처음 실행될 때 어디서부터 읽을지 설정
                # earliest = 토픽에 쌓인 가장 오래된 메시지부터 읽음
                # latest   = 지금 이후에 새로 들어오는 메시지만 읽음
                # → earliest라서 producer가 먼저 실행돼도
                #   consumer가 나중에 켜지면 처음부터 다 읽을 수 있음

                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                # producer의 value_serializer 반대 과정
                # producer: dict → json.dumps → encode('utf-8') → bytes
                # consumer: bytes → decode('utf-8') → json.loads → dict
            
                request_timeout_ms=5000,        # 요청 타임아웃 5초
                connections_max_idle_ms=10000,  # 연결 대기 최대 10초
            )
            # 실제 연결 확인: 브로커 목록 조회를 강제로 시도
            consumer.topics()  # ← 이 시점에 실제 연결 발생 # 타임아웃 안에 연결 못 하면 예외 발생
            print("Kafka 연결 성공!")
            return consumer
        except NoBrokersAvailable:
            print(f"Kafka 준비 중... {i+1}/10 재시도")
            time.sleep(3)
    raise Exception("Kafka 연결 실패")

'''
import datetime
import psycopg2  # PostgreSQL에 직접 연결하는 라이브러리 -- steam_db에 저장예정
import os        # 환경변수 읽기용

import io          # 메모리 상의 파일 객체 생성 (S3 업로드 시 실제 파일 없이 전송)
import pandas as pd   # Parquet 변환용 -- pd.to_parquet()은 내부적으로 pyarrow 사용 -> requirements.txt 추가
import boto3          # S3 업로드용

# ─────────────────────────────────────────
# 설정값
# ─────────────────────────────────────────
BATCH_SIZE = 20  # 메시지 몇 개마다 S3에 올릴지
                 # Steam API 특성상 한 번에 60개 정도 수신됨
                 # 20개씩 묶으면 약 3개 파일 생성

# ─────────────────────────────────────────
# DB 연결
# ─────────────────────────────────────────
def get_db_connection():
    """
    PostgreSQL 연결 반환
    환경변수에서 DB 접속 정보를 읽어옴
    (환경변수는 docker-compose의 environment에서 주입해줄 예정)
    """
    return psycopg2.connect(
        host=os.environ['DB_HOST'],         # postgres 컨테이너 이름
        dbname=os.environ['DB_NAME'],       # steam_db
        user=os.environ['DB_USER'],         # airflow
        password=os.environ['DB_PASSWORD']  # airflow
    )

# ─────────────────────────────────────────
# Kafka Consumer 생성
# ─────────────────────────────────────────
def create_consumer():
    for i in range(10):  # 최대 10번 재시도
        try:
            consumer = KafkaConsumer(
                'steam-deals',
                bootstrap_servers='kafka:9092',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                
            )
            consumer.topics()  # 파티션 assign 완료 강제
            print(f"[{datetime.datetime.now()}] Kafka 연결 성공!")
            # print(f"[{datetime.datetime.now()}] Consumer 시작 - 메시지 대기 중...")
            return consumer
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Kafka 준비 중... {i+1}/10 재시도 ({e})")
            time.sleep(5)  # 5초 대기 후 재시도
    raise Exception("Kafka 연결 실패 - 최대 재시도 초과")

# ─────────────────────────────────────────
# S3 업로드
# ─────────────────────────────────────────
def upload_to_s3(batch: list):
#                       ^^^^
#                  타입 힌트 (Type Hint) - batch는 list 타입이어야 한다는 힌트
    """
    batch: deal dict 리스트
    → DataFrame → Parquet(메모리) → S3 raw/ 업로드
    """
    # S3 클라이언트 생성 (환경변수에서 자동으로 인증정보 읽음)
    s3 = boto3.client(
        's3',
        region_name=os.environ['AWS_REGION']
        # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY는
        # boto3가 환경변수에서 자동으로 읽음 → 명시 불필요
    )

    bucket = os.environ['S3_BUCKET_NAME']

    # 타임스탬프로 고유한 파일명 생성
    # 예: raw/2025-05-01T12:30:45.parquet
    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    s3_key = f"raw/{timestamp}.parquet"

    # dict 리스트 → DataFrame 변환 (consumer는 bytes -> dick 리스트)
    df = pd.DataFrame(batch)
    # 필요한 컬럼만 선택 (DB 저장과 동일한 필드)
    df = df[['title', 'salePrice', 'normalPrice', 'savings']]

    # Parquet을 실제 파일로 저장하지 않고 메모리 버퍼에 씀
    # io.BytesIO() = 메모리 상의 가상 파일 (RAM에만 존재)
    # 이유: 컨테이너 안에 파일 저장 → 컨테이너 재시작 시 사라짐
    #       메모리 버퍼 → S3 직접 전송이 더 안전하고 빠름
    buffer = io.BytesIO()    # RAM에 빈 가상파일 생성
                             # 커서: [ ]
                             #        ↑

    df.to_parquet(buffer, index=False)  # index=False: 행 번호 컬럼 제외
                             # Parquet 데이터를 buffer에 씀
                             # 커서: [✦✦✦✦✦✦✦✦]
                             #                   ↑ 끝으로 이동
    
    buffer.seek(0)  # 버퍼 읽기 위치를 처음으로 되돌림
                    # 파일을 다 쓴 후 커서가 끝에 있어서 seek(0) 안 하면 빈 데이터 전송됨
                             # 커서를 처음으로 되돌림
                             # 커서: [✦✦✦✦✦✦✦✦]
                             #        ↑
                            # seek(0) 빠지면 : 커서가 끝에 있으니까
                             # 커서: [✦✦✦✦✦✦✦✦]
                             #                   ↑ 여기서부터 읽음
                             # → 빈 데이터 전송 ❌

    # S3에 업로드
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,       # S3 내 경로 (raw/타임스탬프.parquet)
        Body=buffer       # 업로드할 데이터 (메모리 버퍼)
                             # 커서 위치부터 읽어서 S3로 전송
                             # → 전체 데이터 정상 전송 ✅
    )

    print(f"[{datetime.datetime.now()}] S3 업로드 완료: s3://{bucket}/{s3_key} ({len(batch)}건)")


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def main():
    conn = get_db_connection()  # DB 연결: conn = psycopg2 DB 연결 객체
    consumer = create_consumer()

    batch = []  # S3 업로드용 임시 누적 리스트

    print(f"[{datetime.datetime.now()}] Consumer 시작 - 메시지 대기 중...")
    for message in consumer:
        # consumer는 이터러블(iterable, 반복 가능한) 객체
        # for문이 계속 돌면서 새 메시지 올 때마다 실행
        # 메시지 없으면 대기, 오면 처리 → 무한루프처럼 동작

        deal = message.value  # producer가 보낸 dict
        # message 객체 구조:
        # message.topic     → 토픽 이름 ('steam-deals')
        # message.partition → 파티션 번호 (0)
        # message.offset    → 이 메시지의 offset 번호
        # message.key       → 메시지 키 (우리는 안 씀)
        # message.value     → 실제 데이터 (producer가 보낸 dict)

        # ── 1. PostgreSQL 저장 (기존 로직 유지) ──
        # with = 블록 끝나면 cursor 자동으로 닫아줌 (메모리 누수 방지)
        with conn.cursor() as cur: # cursor = DB에 SQL 명령어를 전달하는 도구 (마치 DB에 보내는 펜)
            cur.execute("""
                INSERT INTO steam_deals
                    (title, sale_price, normal_price, savings)
                VALUES (%s, %s, %s, %s) 
            """, ( # SQL 인젝션 공격 방어 위해 자리표시자 %s 사용 
                deal['title'],        # 게임 제목
                deal['salePrice'],    # 할인가
                deal['normalPrice'],  # 원가
                deal['savings']       # 할인율
            ))
            conn.commit()  # 한 건씩 즉시 커밋 (데이터 유실 방지)
            # DB는 기본적으로 트랜잭션 단위로 동작함
            # execute()만 하면 임시 저장 상태 → DB에 실제로 안 들어감
            # commit() 해야 확정 저장
            # execute()  →  임시 저장 (롤백 가능)
            # commit()   →  확정 저장 (되돌릴 수 없음)

        # ── 2. 배치 누적 ──
        batch.append(deal)

        # print(f"수신완료: {deal['title']} | 가격: ${deal['salePrice']}")
        # value_deserializer 덕분에
        # message.value가 이미 dict로 변환돼있음
        # 그래서 deal['title'] 바로 접근 가능
        print(f"[{datetime.datetime.now()}] 저장완료: {deal['title']} | ${deal['salePrice']} ({len(batch)}/{BATCH_SIZE})")

        # ── 3. BATCH_SIZE 도달 시 S3 업로드 ──
        if len(batch) >= BATCH_SIZE:
            upload_to_s3(batch)
            batch = []  # 업로드 후 배치 초기화

if __name__ == "__main__":
    main()