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

def create_consumer():
    consumer = KafkaConsumer(
        'steam-deals',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        
    )
    consumer.topics()  # 파티션 assign 완료 강제
    print(f"[{datetime.datetime.now()}] Kafka 연결 성공!")
    print(f"[{datetime.datetime.now()}] Consumer 시작 - 메시지 대기 중...")
    return consumer

def main():
    consumer = create_consumer()
    print("Consumer 시작 - 메시지 대기 중...")
    for message in consumer:
        # consumer는 이터러블(iterable, 반복 가능한) 객체
        # for문이 계속 돌면서 새 메시지 올 때마다 실행
        # 메시지 없으면 대기, 오면 처리 → 무한루프처럼 동작

        deal = message.value
        # message 객체 구조:
        # message.topic     → 토픽 이름 ('steam-deals')
        # message.partition → 파티션 번호 (0)
        # message.offset    → 이 메시지의 offset 번호
        # message.key       → 메시지 키 (우리는 안 씀)
        # message.value     → 실제 데이터 (producer가 보낸 dict)

        print(f"수신완료: {deal['title']} | 가격: ${deal['salePrice']}")
        # value_deserializer 덕분에
        # message.value가 이미 dict로 변환돼있음
        # 그래서 deal['title'] 바로 접근 가능

if __name__ == "__main__":
    main()