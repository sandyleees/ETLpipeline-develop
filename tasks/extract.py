import requests       # API 호출
import pandas as pd  # CSV 저장을 위해 추가
import os            # 폴더 존재 여부 확인을 위해 추가

def extract_deals():
    """
    CheapShark API에서 할인 게임 목록을 가져와
    data/raw.csv로 저장하는 함수
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

    # 딕셔너리 리스트를 DataFrame으로 변환 ----------------------- 여기부터 변화. CSV 저장
    # 기존: return data       → transform이 함수 호출해서 받아씀
    # 새것: data/raw.csv 저장 → transform이 파일 읽어서 사용
    df = pd.DataFrame(data)

    # data 폴더가 없으면 생성
    os.makedirs("data", exist_ok=True)

    # CSV로 저장 (transform.py가 이 파일을 읽어서 사용)
    df.to_csv("data/raw.csv", index=False)
    print("data/raw.csv 저장 완료")


if __name__ == "__main__":
    extract_deals()