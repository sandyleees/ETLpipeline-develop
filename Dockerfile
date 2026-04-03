# 베이스 이미지 (Airflow 공식 이미지)
# Docker Hub에서 apache/airflow:2.8.0 이미지를 베이스로 가져와
# 이 이미지 안에는 이미 Airflow가 설치되어 있음
# 우리는 이 위에 추가 설정만 얹는 거예요
FROM apache/airflow:2.8.0 

USER root
# data 폴더 생성 및 권한 설정
# 컨테이너 안에서 명령어를 실행할 사용자를 root로 변경
# root = 리눅스 최고 관리자 (모든 권한 있음)
# 왜 root로 바꾸냐?
# → 폴더 생성, 권한 변경은 관리자 권한이 필요하거든요
# → airflow 사용자는 이런 작업 권한이 없음
RUN mkdir -p /opt/airflow/data && chmod 777 /opt/airflow/data
# RUN = 이미지 빌드할 때 실행할 명령어
# mkdir -p /opt/airflow/data = data 폴더 생성
#   -p = 상위 폴더도 없으면 같이 생성
# && = 앞 명령어 성공하면 다음 명령어 실행
# chmod 777 /opt/airflow/data = data 폴더 권한을 777로 변경
#   777 = 소유자/그룹/나머지 모두 읽기+쓰기+실행 가능
#   → airflow 사용자도 이 폴더에 파일 쓸 수 있게 됨

USER airflow
# 다시 airflow 사용자로 변경
# 권한 설정 끝났으니 다시 일반 사용자로 돌아옴
# 보안상 root로 계속 실행하는 건 위험하거든요
# → 필요한 작업만 root로 하고 바로 일반 사용자로 돌아오는 게 관례

# 로컬의 requirements.txt를 컨테이너 안으로 RUN 전에 먼저 복사해야 읽을 수 있음
# 로컬 requirements.txt → 컨테이너 /opt/airflow/requirements.txt (컨테이너 안의 현재 작업 디렉토리)
COPY requirements.txt .
# 로컬의 requirements.txt를 컨테이너 현재 작업 디렉토리로 복사
# 현재 작업 디렉토리 = /opt/airflow/ (Airflow 기본 경로)
# COPY 로컬경로 컨테이너경로


# 로컬 venv와 동일한 버전으로 설치
# 로컬에 설치한 버전대로 ETL파이프라인 코드를 짰을테니 
# 로컬의 라이브러리에 맞춰서 이미지 굽기
# 로컬의 라이브러리 버전은 pip show requests, pip show pandas로 확인함
# requests==2.33.1 -> 2.31.0 로 버전 낮춤. ∵ 로컬은 Python 3.14라서 2.33.1 설치됐는데, 컨테이너는 Python 버전이 낮아서 설치 불가
# pandas==3.0.2 -> Python 3.8(컨테이너 환경)에서 설치 가능한 pandas 최신버전 2.0.3 으로 버전낮춤           
#  => requirements.txt에 넣어서 관리 


# --no-cache-dir = 설치 후 캐시 삭제 (이미지 용량 줄이기 위해)
# 복사된 requirements.txt 읽어서 라이브러리 설치
# -r = 파일을 읽어서 설치
# requirements.txt만 관리하면 됨 → 가장 깔끔 
RUN pip install --no-cache-dir -r requirements.txt 
