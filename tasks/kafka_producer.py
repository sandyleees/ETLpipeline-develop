import json       # Python 내장 라이브러리 (설치 불필요)
                  # dict ↔ JSON 문자열 변환 담당
import time      # Python 내장 라이브러리 (설치 불필요)
                  # time.sleep() 으로 대기
import requests   # requirements.txt 에 있던 것 (이미 설치됨)
                  # HTTP 요청 담당
from kafka import KafkaProducer # pip install kafka-python 으로 설치한 것
from kafka.admin import KafkaAdminClient, NewTopic
# KafkaAdminClient → Kafka 관리 작업을 하는 클라이언트
#                    토픽 생성/삭제 같은 관리 작업 담당
#                    producer.py의 KafkaProducer는 메시지 전송 전용
#                    역할이 달라서 별도로 import

# NewTopic → 새로 만들 토픽의 설정을 담는 객체
#            이름, 파티션 수, 복제본 수를 지정
from kafka.errors import TopicAlreadyExistsError
# Kafka 관련 에러 모음에서 TopicAlreadyExistsError만 가져옴
# 토픽이 이미 있을 때 create_topics() 가 이 에러를 던짐
# 미리 import 해둬야 except 절에서 잡을 수 있음

# ── 토픽 생성 ────────────────────────────────────────────────────
def create_topic():
    admin = KafkaAdminClient(bootstrap_servers='kafka:9092')
     # KafkaProducer랑 똑같이 브로커 주소로 연결 (컨테이너 이름으로 접속)
    # 관리 작업 전용 클라이언트
    try:
        admin.create_topics([NewTopic(
            name='steam-deals', # 토픽 이름
            num_partitions=1,   # 파티션 개수
            replication_factor=1 # 복제본 개수 (브로커 1대라 1)
        )])
        # create_topics()는 리스트를 받음
        # 한번에 여러 토픽 만들 수 있어서 []로 감쌈
        print("토픽 생성 완료: steam-deals")
    except TopicAlreadyExistsError:
        # 토픽이 이미 있으면 에러 대신 그냥 넘어감
        # producer.py를 두 번 실행해도 문제 없도록
        print("토픽 이미 존재: steam-deals")
    except Exception as e:
        # 다른 모든 에러 출력
        print(f"토픽 생성 에러: {type(e).__name__}: {e}")
    finally:
        admin.close()
        # try/except 결과와 상관없이 항상 실행
        # 연결 자원을 반드시 해제
        # finally 없으면 에러 발생 시 연결이 안 닫힐 수 있음

# ── Kafka Producer 설정 ──────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # 접속할 브로커 주소 (컨테이너 이름으로 접속)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # value_serializer: 메시지를 bytes로 변환하는 함수
    # Kafka는 bytes만 전송 가능 → dict를 JSON 문자열로 변환 후 UTF-8로 인코딩
    # 1단계 : json.dumps(v) # dict → JSON 문자열 (dump string)
    # 2단계: .encode('utf-8') # JSON 문자열 → bytes # 이 bytes를 Kafka가 전송
)
# Consumer에서 Kafka로부터 메시지를 받을 때 json.loads() 로 다시 bytes -> dict로 변환할 예정

# ── Steam API 호출 ───────────────────────────────────────────────
def fetch_steam_deals():
    url = "https://www.cheapshark.com/api/1.0/deals"
    params = {
        "storeID": "1",   # Steam 스토어
        "pageSize": 10    # 10개 가져오기
    }
    response = requests.get(url, params=params)
    return response.json()

# ── 메시지 전송 ──────────────────────────────────────────────────
def main():
    create_topic()  # 토픽 없으면 만들고, 있으면 그냥 넘어감
                   # 이 한 줄 덕분에 kafka-python의
                    # 자동생성 미지원 문제 해결
    print("Producer 시작")

    deals = fetch_steam_deals()
    print(f"Steam에서 {len(deals)}개 딜 가져옴")

    for deal in deals:
        # steam-deals 토픽으로 딜 데이터 전송
        producer.send('steam-deals', value=deal)
        # 메시지 즉시 kafka 전송 X, 일단 producer 내부 버퍼에 쌓임
        # 버퍼에 일정량 쌓이거나 일정 시간 지나면 kafka 브로커로 한 번에 전송 (∵ 네트워크 효율)
        print(f"전송완료: {deal['title']}")
        time.sleep(0.5)  # 0.5초 간격으로 전송 -- 학습용. 없어도 동작함

    # 버퍼에 남은 메시지 전부 전송 후 종료
    # 프로그램 끝나기 전에 버퍼 비워서
    # 전송 안 된 메시지 없이 마무리
    producer.flush()
    print("전송 완료")


if __name__ == "__main__":
    main()