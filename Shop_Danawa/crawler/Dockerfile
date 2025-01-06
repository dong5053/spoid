# Python 3.11 이미지 사용
FROM python:3.11-slim

# 필요한 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    gnupg \
    sudo \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libu2f-udev \
    libvulkan1 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends

RUN apt-get install -y chromium-driver

# 구글 크롬 설치
RUN wget https://dl.google.com/linux/deb/pool/main/g/google-chrome-stable/google-chrome-stable_125.0.6422.141-1_amd64.deb \
    && sudo dpkg -i google-chrome-stable_125.0.6422.141-1_amd64.deb \
    && apt-get install -f -y \
    && rm google-chrome-stable_125.0.6422.141-1_amd64.deb

# ChromeDriver 설치
#RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}') \
#    && CHROMEDRIVER_VERSION=$(curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION) \
#    && wget -N https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip -P /tmp/ \
#    && unzip /tmp/chromedriver_linux64.zip -d /usr/local/bin/ \
#    && rm /tmp/chromedriver_linux64.zip \
#    && chmod +x /usr/local/bin/chromedriver

# 환경 변수 설정
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROME_DRIVER=/app/chromedriver-linux

# 작업 디렉토리 설정
WORKDIR /app

# 필요 파일 복사 및 Python 패키지 설치
COPY chromedriver-linux /app/chromedriver-linux
RUN chmod 777 /app/chromedriver-linux

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 코드 복사
COPY . /app

# 엔트리포인트 설정
CMD ["python3", "shop_danawa_crawler.py"]









# FROM python:3.11-slim

# RUN apt-get update && apt-get install -y \
#     wget \
#     unzip \
#     curl \
#     gnupg \
#     sudo \
#     --no-install-recommends

# RUN apt --fix-broken install -y

# # 구글 크롬 설치
# RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
#     && sudo dpkg -i google-chrome-stable_current_amd64.deb \
#     && apt-get install -f -y \
#     && rm google-chrome-stable_current_amd64.deb

# WORKDIR /app

# COPY requirements.txt requirements.txt
# RUN pip install -r requirements.txt

# COPY . .

# CMD ["python", "shop_danawa_crawler.py"]

