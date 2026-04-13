from airflow.decorators import task  # @task 데코레이터 가져오기
                                     # BashOperator 대신 이걸 써요
import requests
import pandas as pd

@task  # ← 이 한 줄이 핵심!
       # 이 함수 자체가 Airflow Task가 됨
       # BashOperator로 외부에서 실행하던 걸
       # 함수 위에 @task 붙이는 것으로 대체
def extract():
    """
    CheapShark API에서 할인 게임 목록을 가져와
    데이터를 return하는 함수

    [기존 extract.py와 다른 점]
    기존 : 결과를 data/raw.csv 파일로 저장
    새것 : 결과를 return → Airflow가 자동으로 XCom에 저장
           다음 Task(transform)가 XCom에서 꺼내 씀
    """
    url = "https://www.cheapshark.com/api/1.0/deals"

    params = {
        "upperPrice": 30,
        "pageSize": 20,
        "sortBy": "DealRating",
        "storeID": "1"
    }

    print("CheapShark API 호출 중...")
    response = requests.get(url, params=params)
    print(f"응답 상태코드: {response.status_code}")

    data = response.json()
    print(f"가져온 게임 수: {len(data)}개")

    # [기존 extract.py]
    # df = pd.DataFrame(data)
    # df.to_csv("data/raw.csv", index=False)  ← 파일로 저장

    # [새것 extract_taskflow.py]
    # return하면 Airflow가 자동으로 XCom에 저장해줌
    # 다음 Task가 이 return값을 인자로 바로 받아 씀
    return data  # ← CSV 저장 대신 return!