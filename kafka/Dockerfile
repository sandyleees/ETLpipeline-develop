FROM python:3.11-slim

WORKDIR /app

# 라이브러리 설치
COPY requirements.txt .
RUN pip install -r requirements.txt

# 코드 복사
COPY producer.py .
COPY consumer.py .