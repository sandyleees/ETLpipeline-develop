import pandas as pd
from datetime import datetime
import os

def transform_deals():
    """
    data/raw.csv를 읽어서 정제 후
    data/clean.csv로 저장하는 함수
    (extract.py에 대한 의존성 없음)
    """

    # extract.py가 저장한 CSV 파일을 읽어옴
    # → import 의존성 없이 파일로 데이터를 주고받음 --------------- ★
    df = pd.read_csv("data/raw.csv")
    # 항상 루트폴더(steam-etl-v2)에서 실행해야 data 폴더 찾을 수 있다.
    # cd steam-etl-v2하고 python3 tasks/transform.py 실행하면 찾을 수 있음 
    # cd steam-etl-v2/task에서 python3 transform.py 실행하면 못 찾는다 (tasks 내에 data폴더 없음)

    print(f"정제 전 컬럼 목록: {list(df.columns)}")
    print(f"정제 전 행 수: {len(df)}")

    # ① 필요한 컬럼만 추리기
    df = df[[
        'title',
        'salePrice',
        'normalPrice',
        'savings',
        'metacriticScore',
        'steamRatingText',
        'dealRating',
        'releaseDate'
    ]]

    # ② 데이터 타입 변환
    df['salePrice']       = df['salePrice'].astype(float)
    df['normalPrice']     = df['normalPrice'].astype(float)
    df['savings']         = df['savings'].astype(float)
    df['metacriticScore'] = df['metacriticScore'].astype(int)
    df['dealRating']      = df['dealRating'].astype(float)

    # ③ 소수점 정리
    df['savings'] = df['savings'].round(1)

    # ④ 타임스탬프 → 날짜 변환
    df['releaseDate'] = df['releaseDate'].apply(
        lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d')
    )

    print(f"\n정제 후 컬럼 목록: {list(df.columns)}")
    print(f"정제 후 행 수: {len(df)}")

    # 정제된 데이터를 clean.csv로 저장  --------------- ★
    # → load.py가 이 파일을 읽어서 사용
    os.makedirs("data", exist_ok=True) # data 폴더가 없으면 생성
    df.to_csv("data/clean.csv", index=False) # CSV로 저장 (load.py가 이 파일을 읽어서 사용)
    print("data/clean.csv 저장 완료")


if __name__ == "__main__":
    transform_deals()

    # 결과 확인
    df = pd.read_csv("data/clean.csv")
    print("\n정제된 데이터 미리보기:")
    print(df.head())
    print("\n데이터 타입 확인:")
    print(df.dtypes)
