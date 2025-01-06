# Python 3.11 이미지 사용
FROM python:3.11-slim

RUN apt-get update

# 작업 디렉토리 설정
WORKDIR /app

# 필요 파일 복사 및 Python 패키지 설치
# 코드 복사
COPY . .
#COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


# 엔트리포인트 설정
CMD ["python3", "shopdanawa-gpu-preprocessor.py"]