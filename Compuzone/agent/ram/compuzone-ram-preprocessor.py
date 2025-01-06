import pandas as pd
import re
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def convert_compuzone_ram(data, ram_standard):
    # 기준 데이터 전처리
    def process_line(row):
        product = row.get('name', '')
        company = row.get('company', '')
        subtext = row.get('spec', '')
        image_url = row.get('img', '')
        price = row.get('price', '')
        product_url = row.get('link', '')
        #product = row['Product']
        #company = row['Company']
        #subtext = row['subtext']
        #image_url = row['img']
        #price = row['Price']
        #product_url = row['link']

        # 서버용 및 노트북 데이터 제거
        if any(word in product for word in ["서버용", "노트북", "산업용"]):
            return None

        # 제조사 추출
        manufacturer = company.upper().replace(' ', '')

        # 모델명 추출
        # 제조사명이 Product에 있으면 제거
        product = re.sub(rf'\[{manufacturer}\]', '', product, flags=re.IGNORECASE).strip()
        product = re.sub(r'\[.*?\]', '', product).strip()  # 대괄호 내 정보 제거
        model = product.upper()

        # 불필요한 정보 제거
        model = re.sub(r'\(.*?\)', '', model).strip()  # 소괄호 내 정보 제거
        model = re.sub(r'\bDDR\d\b', '', model).strip()  # DDR4, DDR5 등 제거
        model = re.sub(r'\bPC\d+-\d+\b', '', model).strip()  # PC4-25600 등 제거
        model = re.sub(r'\-\d+', '', model).strip()  # -48000 등 제거
        model = re.sub(r'\bCL\d+\b', '', model).strip()  # CL19 등 제거
        model = re.sub(r'[\u2600-\u26FF\u2700-\u27BF]+', '', model).strip()  # 특수문자 제거
        model = re.sub(r'\s*\*\d*\*\s*', '', model).strip()  # *2* 같은 정보 제거
        model = re.sub(r'\s+', ' ', model).strip()  # 다중 공백 제거

        # 특정 단어 제거
        for word in ["삼성", "팀그룹", "SK하이닉스", "게일", "패트리어트", "방열판", "파인인포", "이케이메모리", "인디", "트랜센드", "킹스톤", "씨넥스"]:
            model = re.sub(rf'\b{word.upper()}\b', '', model).strip()

        # 제조사명이 모델명에 있으면 제거
        model = re.sub(rf'\b{manufacturer}\b', '', model, flags=re.IGNORECASE).strip()
        
        # 모델명에서 용량 제거
        model = re.sub(r'\b\d+GB\b', '', model).strip()

        # 용량 추출
        capacity_match = re.search(r'(\d+)GB', subtext)
        capacity = capacity_match.group(1) if capacity_match else 'Unknown'

        # 규격 및 동작속도 추출
        spec_match = re.search(r'(DDR\d)', subtext)
        spec = spec_match.group(1) if spec_match else 'Unknown'
        speed_match = re.search(r'(\d+)MHz', subtext) or re.search(r'\((\d+)MHz', subtext)
        speed = speed_match.group(1) if speed_match else 'Unknown'

        # RGB 지원 여부 확인 및 모델명에서 제거
        rgb_support = 'RGB' in model
        if rgb_support:
            model = model.replace('RGB', '').strip()

        # 색상 정보 추출 및 모델명에서 제거
        color_map = {
            '블랙': 'BLACK', 'BLACK': 'BLACK',
            '화이트': 'WHITE', 'WHITE': 'WHITE',
            '블루': 'BLUE', 'BLUE': 'BLUE',
            '골드': 'GOLD', 'GOLD': 'GOLD',
            '실버': 'SILVER', 'SILVER': 'SILVER',
            '핑크': 'PINK', 'PINK': 'PINK',
            '레드': 'RED', 'RED': 'RED',
            '그린': 'GREEN', 'GREEN': 'GREEN',
            '인디핑크': 'INDIPINK', 'INDIPINK': 'INDIPINK',
            '그레이': 'GRAY', 'GRAY': 'GRAY'
        }
        colors = []
        for kr_color, en_color in color_map.items():
            if re.search(fr'\b{kr_color}\b', model, re.IGNORECASE):
                colors.append(en_color)
                model = re.sub(fr'\b{kr_color}\b', '', model, flags=re.IGNORECASE).strip()

        color_field = ' '.join(sorted(set(colors)))

        # 모델명에서 공백 1개로 통일
        model = re.sub(' +', ' ', model)

        # 이미지 URL이 //로 시작하면 https: 추가
        if image_url.startswith('//'):
            image_url = 'https:' + image_url

        # 가격 필드에서 숫자만 추출
        price = re.sub(r'[^\d]', '', price)

        return {
            "manufacturer": manufacturer,
            "model": model,
            "capacity": capacity,
            "price": price,
            "spec": spec,
            "speed": speed,
            "image_url": image_url,
            "product_url": product_url,
            "color_field": color_field,
            "rgb_support": rgb_support
        }

    # 데이터 처리
    processed_data = []
    for _, row in data.iterrows():
        processed_line = process_line(row)
        if processed_line:
            processed_data.append(processed_line)

    processed_df = pd.DataFrame(processed_data)

    # 기본값 설정: 필요한 열이 누락되었을 경우 기본값으로 채움
    for col in ['manufacturer', 'model', 'spec', 'speed', 'color_field', 'price']:
        if col not in processed_df.columns:
            processed_df[col] = 'UNKNOWN'

    processed_df['speed'] = processed_df['speed'].astype(str)  # speed 열을 문자열로 변환

    # 중복 행 제거
    processed_df.drop_duplicates(inplace=True)
    
    # Filter processed data based on ram_standard
    ram_standard['speed'] = ram_standard['speed'].astype(str)  # ram_standard의 speed 열도 문자열로 변환
    

    # 디버깅: 데이터프레임 열 출력
    print("processed_df columns:", processed_df.columns)
    print("ram_standard columns:", ram_standard.columns)

    merged_df = processed_df.merge(ram_standard, on=['manufacturer', 'model', 'spec', 'speed'], how='inner')

     # 이미지 URL을 병합된 데이터프레임에 추가
    merged_df = merged_df.merge(processed_df[['manufacturer', 'model', 'spec', 'speed', 'product_url']], on=['manufacturer', 'model', 'spec', 'speed'], how='left', suffixes=('', '_y'))
    
    # 중복 데이터 제거
    # 부품구분_제조사_모델명_규격_동작속도_색상
    merged_df.drop_duplicates(subset=['manufacturer', 'model', 'spec', 'speed', 'color_field', 'product_url'], inplace=True)

    # Add necessary columns
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged_df['ComponentID'] = 'RAM#' + merged_df['manufacturer'] + '#' + merged_df['model'] + '#' + merged_df['spec'] + '#' + merged_df['speed'] + '#' + merged_df['color_field']
    merged_df['Type'] = 'RAM'
    merged_df['Date'] = current_time
    merged_df['Shop'] = 'compuzone'
    #merged_df['URL'] = 'https://spoid-components.s3.ap-northeast-2.amazonaws.com/RAM/RAM_' + merged_df['manufacturer'] + '_' + merged_df['model'] + '_' + merged_df['spec'] + '_' + merged_df['speed'] + '_' + merged_df['color_field'] + '.jpg'
    merged_df['URL'] = merged_df['product_url']

    # 디버깅: 병합 후 데이터프레임 열 출력
    print("merged_df columns:", merged_df.columns)

    # Select necessary columns and rename for final JSON output
    processed_final_df = merged_df[['ComponentID', 'Type', 'Date', 'Shop', 'price', 'URL']].copy()
    processed_final_df.rename(columns={'price': 'Price'}, inplace=True)
    
    # Convert to JSON
    processed_json = processed_final_df.to_dict(orient='records')
    
    return processed_json

# Kafka 설정
RAW_TOPIC = 'raw-data.compuzone.ram.json'
PROCESSED_TOPIC = 'processed-data.compuzone.ram.json'

bootstrap_servers = [
    'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
    'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
    'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
]

# Kafka Consumer 생성
consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    # earliest
    enable_auto_commit=True,
    group_id='compuzone-ram-preprocessor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# RAM standard CSV 불러오기
ram_standard = pd.read_csv('/app/ram_standard.csv')

# 배치 처리
while True:
    raw_messages = consumer.poll(timeout_ms=2000)
    batch = []
    for tp, messages in raw_messages.items():
        for message in messages:
            raw_data = message.value
            batch.append(raw_data)
    
    if batch:
        data_df = pd.DataFrame(batch)
        processed_data = convert_compuzone_ram(data_df, ram_standard)
        if processed_data:
            for data in processed_data:
                producer.send(PROCESSED_TOPIC, value=json.dumps(data))
            producer.flush()
