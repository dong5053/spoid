import pandas as pd
import re
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def convert_danawa_ram(data_df, ram_standard):
    # 기준 데이터 전처리
    def process_line(row):
        manufacturer_model = row.get('productName', '')
        capacity_field = row.get('productPriceStr', '')
        description = row.get('productSpec', '')
        image_url = row.get('productImage', '')
        product_url = row.get('productUrlAll', '')

        # 이미지 URL이 //로 시작하면 https: 추가
        if image_url.startswith('//'):
            image_url = 'https:' + image_url

        # 제조사, 모델명
        manufacturer = manufacturer_model.split(' ')[0].upper()
        model = ' '.join(manufacturer_model.split(' ')[1:]).upper()

        # 색상 정보를 추출하고 모델명에서 제거
        color_map = {
            '블랙': 'BLACK', 'BLACK': 'BLACK',
            '화이트': 'WHITE', 'WHITE': 'WHITE',
            '블루': 'BLUE', 'BLUE': 'BLUE',
            '골드': 'GOLD', 'GOLD': 'GOLD',
            '실버': 'SILVER', 'SILVER': 'SILVER',
            '핑크': 'PINK', 'PINK': 'PINK',
            '레드': 'RED', 'RED': 'RED',
        }
        color = None
        for kr, en in color_map.items():
            if kr in model or en in model:
                color = en
                model = model.replace(kr, '').replace(en, '').strip()
                break
        
        # RGB 지원 여부 확인 및 모델명에서 제거
        rgb_support = 'RGB' in model
        if rgb_support:
            model = model.replace('RGB', '').strip()

        # 아래 단어들을 제거
        for word in ["중고", "병행수입", "해외구매", "패키지"]:
            model = model.replace(word, "").strip()

        # 각 단어 사이에 공백은 1개로 통일
        model = re.sub(' +', ' ', model)

        # 추출 : usage, spec, speed, timing, and XMP
        description_parts = description.split(' / ')
        usage = description_parts[0]
        if usage != "데스크탑용":
            return None

        spec = description_parts[1]
        speed_match = re.search(r'(\d+)MHz', description)
        speed = speed_match.group(1) if speed_match else None
        timing_match = re.search(r'CL\d+(-\d+)*', description)
        timing = timing_match.group(0) if timing_match else None
        
        ram_type_match = re.search(r'(DDR\d+)', description)
        ram_type = ram_type_match.group(1) if ram_type_match else None
        
        # 모델명에서도 램타이밍 값 추출 시도
        if not timing:
            timing_match = re.search(r'CL\d+(-\d+)*', model)
            timing = timing_match.group(0) if timing_match else None
        
        # 램타이밍에서 첫 번째 값만 추출
        if timing:
            timing = timing.split('-')[0]

        xmp = 'XMP' in description
        
        # 모델명에서 규격, 램타이밍, 동작 속도, 전압 제거
        model = re.sub(r'\bDDR\d-\d+ CL\d+\b', '', model).strip()
        model = re.sub(r'\bDDR\d\b', '', model).strip()
        model = re.sub(r'-\d+', '', model).strip()  # 동작 속도 제거
        model = re.sub(r'\d.\d+V', '', model).strip()  # 전압 제거
        model = re.sub(r' +', ' ', model)

        # 용량과 가격 추출
        capacity_price_list = []
        items = capacity_field.split('|')
        for item in items:
            match = re.search(r'(\d+GB).*?/1GB_(\d+,?\d*)', item)
            if match:
                capacity = match.group(1).replace('GB', '').strip()
                price = match.group(2).replace(',', '').strip()
                capacity_price_list.append({
                    "capacity": capacity,
                    "price": price,
                    "manufacturer": manufacturer,
                    "model": model,
                    "spec": ram_type,
                    "speed": speed,
                    "image_url": image_url,
                    "product_url": product_url,
                    "color_field": color,
                    "rgb_support": rgb_support
                })
        
        return capacity_price_list

    # 데이터 처리
    processed_data = []
    for _, row in data_df.iterrows():
        processed_lines = process_line(row)
        if processed_lines:
            processed_data.extend(processed_lines)

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

    # 디버깅: 병합 전 데이터프레임 내용 출력
    print("processed_df preview:")
    print(processed_df.head())
    print("ram_standard preview:")
    print(ram_standard.head())

    merged_df = processed_df.merge(ram_standard, on=['manufacturer', 'model', 'spec', 'speed'], how='inner')
    
     # 제품 URL을 병합된 데이터프레임에 추가
    merged_df = merged_df.merge(processed_df[['manufacturer', 'model', 'spec', 'speed', 'product_url']], on=['manufacturer', 'model', 'spec', 'speed'], how='left', suffixes=('', '_y'))

    # 중복 데이터 제거
    # 부품구분_제조사_모델명_규격_동작속도_색상
    merged_df.drop_duplicates(subset=['manufacturer', 'model', 'spec', 'speed', 'color_field', 'product_url'], inplace=True)

    # 디버깅: 병합 후 데이터프레임 내용 출력
    print("merged_df preview:")
    print(merged_df.head())

    # Add necessary columns
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged_df['ComponentID'] = 'RAM#' + merged_df['manufacturer'].fillna('') + '#' + merged_df['model'].fillna('') + '#' + merged_df['spec'].fillna('') + '#' + merged_df['speed'].fillna('') + '#' + merged_df['color_field'].fillna('')
    merged_df['Type'] = 'RAM'
    merged_df['Date'] = current_time
    merged_df['Shop'] = 'danawa'
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
RAW_TOPIC = 'raw-data.danawa.ram.json'
PROCESSED_TOPIC = 'processed-data.danawa.ram.json'

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
    enable_auto_commit=True,
    group_id='danawa-ram-processor',
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
    raw_messages = consumer.poll(timeout_ms=3000)
    batch = []
    for tp, messages in raw_messages.items():
        for message in messages:
            raw_data = message.value
            batch.append(raw_data)
    
    if batch:
        data_df = pd.DataFrame(batch)
        processed_data = convert_danawa_ram(data_df, ram_standard)
        if processed_data:
            for data in processed_data:
                producer.send(PROCESSED_TOPIC, value=json.dumps(data))
            producer.flush
