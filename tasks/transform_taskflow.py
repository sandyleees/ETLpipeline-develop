from airflow.decorators import task  # @task 데코레이터
import pandas as pd
from datetime import datetime

@task # 이 함수 자체가 Airflow Task가 됨
def transform(data):  # ← extract()의 return값을 인자로 받음
                      # XCom에 저장된 데이터를 Airflow가 자동으로 여기에 넣어줌
    """
    extract()에서 받은 raw 데이터를 정제하는 함수

    [기존 transform.py와 다른 점]
    기존 : data/raw.csv 파일을 읽어서 → data/clean.csv로 저장
    새것 : extract()의 return값을 인자로 받아서 → return으로 넘김
           파일 저장 없이 XCom으로 데이터 전달
    """

    # 딕셔너리 리스트 → DataFrame 변환
    df = pd.DataFrame(data)

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
    print(f"정제 후 데이터 수: {len(df)}개")

    # [기존 transform.py]
    # df.to_csv("data/clean.csv", index=False)  ← 파일로 저장

    # [새것 transform_taskflow.py]
    # DataFrame을 딕셔너리 리스트로 변환해서 return
    # XCom은 JSON 형태로 저장하기 때문에
    # DataFrame을 그대로 return하면 안 되고 변환해야 함
    return df.to_dict(orient='records')
    # orient='records' = [{컬럼:값, ...}, {컬럼:값, ...}, ...]
    # 딕셔너리 리스트 형태로 변환