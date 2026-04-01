import pandas as pd
import os

def load_to_csv():
    """
    data/clean.csv를 읽어서
    data/steam_deals.csv로 저장하는 함수
    (extract, transform에 대한 의존성 없음)
    """

    # transform.py가 저장한 clean.csv를 읽어옴 -------------- ★
    # → import 의존성 없이 파일로 데이터를 주고받음
    df = pd.read_csv("data/clean.csv")
    # 항상 루트폴더(steam-etl-v2)에서 실행해야 data 폴더 찾을 수 있다.
    # cd steam-etl-v2하고 python3 tasks/load.py 실행하면 찾을 수 있음 
    # cd steam-etl-v2/task에서 python3 load.py 실행하면 못 찾는다 (tasks 내에 data폴더 없음)

    # 저장할 파일 경로
    output_path = "data/steam_deals.csv"

    # DataFrame을 CSV로 저장
    # index=False → 행 번호(0,1,2...)는 저장 안 함
    # encoding='utf-8-sig' → 엑셀에서 열 때 한글 깨짐 방지
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"저장 완료: {output_path}")
    print(f"저장된 행 수: {len(df)}개")


if __name__ == "__main__":
    load_to_csv()
