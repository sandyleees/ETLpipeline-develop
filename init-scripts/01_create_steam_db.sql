-- PostgreSQL은 컨테이너 안에 DB를 여러 개 만들 수 있어요
-- 이 파일은 컨테이너 첫 실행 시 자동으로 실행됨
-- airflow 메타데이터 저장용 DB : airflow (그대로 유지)
-- steam_db 새로 추가 (ETL 결과 저장용 -- 지금은 CSV 파일로데이터 전달중임)

-- steam_db가 없으면 새로 만들어라
-- (이미 있으면 오류 안 나게 IF NOT EXISTS 사용)
SELECT 'CREATE DATABASE steam_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'steam_db'
)\gexec
-- 1단계: SELECT가 문자열을 만들어냄
--   steam_db가 없으면 → 'CREATE DATABASE steam_db' 라는 문자열 반환
--   steam_db가 있으면 → 아무것도 반환 안 함

-- 2단계: \gexec 가 그 문자열을 SQL로 실행해줌
--   'CREATE DATABASE steam_db' → 실제로 실행!
--   아무것도 없으면 → 아무것도 실행 안 함

-- 왜 이렇게 복잡하게 하냐?
-- PostgreSQL은 CREATE DATABASE IF NOT EXISTS 문법이 없어서
-- 이미 있는 DB를 또 만들려고 하면 에러가 나요
-- 그래서 이 방식으로 "없을 때만 만들어" 를 구현하는 거예요


-- steam_db에 접속해서 테이블 생성
\c steam_db
-- \c = connect (접속)
-- "지금부터 steam_db에 접속해서 작업할게" 라는 뜻

-- 왜 필요하냐?
-- SQL 파일은 처음에 기본 DB(airflow)에 접속된 상태로 시작해요
-- \c steam_db 없이 CREATE TABLE 하면
-- airflow DB에 테이블이 만들어져버려요!

-- 그래서 순서가:
-- 1. steam_db 생성
-- 2. \c steam_db  ← "이제 여기로 들어갈게"
-- 3. CREATE TABLE ← steam_db 안에 테이블 생성

-- 게임 딜 정보를 저장할 테이블
-- (아직 없으면 만들어라)
CREATE TABLE IF NOT EXISTS steam_deals (
    id              SERIAL PRIMARY KEY,     -- 자동 증가 고유번호
    title           TEXT,                   -- 게임 제목
    sale_price      NUMERIC(10, 2),         -- 할인가
    normal_price    NUMERIC(10, 2),         -- 원가
    savings         NUMERIC(5, 1),          -- 할인율 (%)
    metacritic_score INT,                   -- 메타크리틱 점수
    steam_rating    TEXT,                   -- 스팀 평점 텍스트
    deal_rating     NUMERIC(5, 2),          -- 딜 평점
    release_date    TEXT,                   -- 출시일
    loaded_at       TIMESTAMP DEFAULT NOW() -- 적재 시각 (자동)
);