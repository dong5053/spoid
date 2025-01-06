FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 패키지 설치
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY shop-danawa-ram-sink-connector.py ./

# 컨테이너 실행 시 실행할 명령
CMD ["python", "shop-danawa-ram-sink-connector.py"]
