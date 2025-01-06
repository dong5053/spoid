import pandas as pd
import re
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def convert_shopdanawa_ram(data, ram_standard):
    data = pd.DataFrame(data)
    
    def process_line(row):
        product = row.get('name', '')
        price = row.get('price', '')
        image_url = row.get('image_url', '')
        product_url = row.get('product_url', '')

        if any(word in product for word in ["중고", "해외구매", "노트북"]):
            return None

        manufacturer = product.split(' ')[0].upper() if len(product.split(' ')) > 0 else 'UNKNOWN'
        model = ' '.join(product.split(' ')[1:]).upper() if len(product.split(' ')) > 1 else 'UNKNOWN'

        capacity_match = re.search(r'\((\d+)GB', product)
        capacity = capacity_match.group(1) if capacity_match else 'Unknown'
        if capacity != 'Unknown':
            model = re.sub(r'\(\d+GB.*?\)', '', model).strip()
            model = re.sub(r'\)', '', model).strip()

        spec_match = re.search(r'(DDR\d)-(\d+)', model)
        spec = spec_match.group(1) if spec_match else 'UNKNOWN'
        speed = spec_match.group(2) if spec_match else 'UNKNOWN'

        if spec != 'UNKNOWN' and speed != 'UNKNOWN':
            model = re.sub(rf'\b{spec}-{speed}\b', '', model).strip()

        rgb_support = 'RGB' in model
        if rgb_support:
            model = model.replace('RGB', '').strip()

        model = re.sub(r'\bCL\d+(-\d+)*', '', model).strip()

        color_map = {
            '블랙': 'BLACK', 'BLACK': 'BLACK',
            '화이트': 'WHITE', 'WHITE': 'WHITE',
            '블루': 'BLUE', 'BLUE': 'BLUE',
            '골드': 'GOLD', 'GOLD': 'GOLD',
            '실버': 'SILVER', 'SILVER': 'SILVER',
            '핑크': 'PINK', 'PINK': 'PINK',
            '레드': 'RED', 'RED': 'RED',
            '그린': 'GREEN', 'GREEN': 'GREEN',
            '그레이': 'GRAY', 'GRAY': 'GRAY'
        }
        colors = []
        for kr_color, en_color in color_map.items():
            if re.search(fr'\b{kr_color}\b', model, re.IGNORECASE):
                colors.append(en_color)
                model = re.sub(fr'\b{kr_color}\b', '', model, flags=re.IGNORECASE).strip()

        color_field = ' '.join(sorted(set(colors)))

        for word in ["병행수입", "패키지"]:
            model = model.replace(word, "").strip()

        model = re.sub(' +', ' ', model)

        if image_url.startswith('//'):
            image_url = 'https:' + image_url

        price = re.sub(r'[^\d]', '', price)

        return {
            "manufacturer": manufacturer if manufacturer else 'UNKNOWN',
            "model": model if model else 'UNKNOWN',
            "capacity": capacity if capacity else 'Unknown',
            "price": price if price else '0',
            "spec": spec if spec else 'UNKNOWN',
            "speed": speed if speed else 'UNKNOWN',
            "image_url": image_url if image_url else '',
            "product_url": product_url if product_url else '',
            "color_field": color_field if color_field else 'UNKNOWN',
            "rgb_support": rgb_support
        }

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
    merged_df['Shop'] = 'shopdanawa'
    merged_df['URL'] = merged_df['product_url']
    #merged_df['URL'] = 'https://spoid-components.s3.ap-northeast-2.amazonaws.com/RAM/RAM_' + merged_df['manufacturer'] + '_' + merged_df['model'] + '_' + merged_df['spec'] + '_' + merged_df['speed'] + '_' + merged_df['color_field'] + '.jpg'

    # 디버깅: 병합 후 데이터프레임 열 출력
    print("merged_df columns:", merged_df.columns)

    # Select necessary columns and rename for final JSON output
    processed_final_df = merged_df[['ComponentID', 'Type', 'Date', 'Shop', 'price', 'URL']].copy()
    processed_final_df.rename(columns={'price': 'Price'}, inplace=True)
    
    # Convert to JSON
    processed_json = processed_final_df.to_dict(orient='records')
    
    return processed_json

# Kafka 설정
RAW_TOPIC = 'raw-data.shop-danawa.ram.json'
PROCESSED_TOPIC = 'processed-data.shop-danawa.ram.json'

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
    group_id='shopdanawa-ram-processor',
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
        processed_data = convert_shopdanawa_ram(batch, ram_standard)
        if processed_data:
            for data in processed_data:
                producer.send(PROCESSED_TOPIC, value=json.dumps(data))
            producer.flush()
