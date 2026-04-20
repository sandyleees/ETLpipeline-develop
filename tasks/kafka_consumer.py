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

def main():
    conn = get_db_connection()  # DB 연결: conn = psycopg2 DB 연결 객체
    consumer = create_consumer()
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

        # print(f"수신완료: {deal['title']} | 가격: ${deal['salePrice']}")
        # value_deserializer 덕분에
        # message.value가 이미 dict로 변환돼있음
        # 그래서 deal['title'] 바로 접근 가능
        print(f"[{datetime.datetime.now()}] 저장완료: {deal['title']} | ${deal['salePrice']}")

if __name__ == "__main__":
    main()