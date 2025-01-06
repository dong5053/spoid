import pandas as pd
import re
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# Kafka 설정
RAW_TOPIC = 'raw-data.danawa.gpu.json'
PROCESSED_TOPIC = 'processed-data.danawa.gpu.json'

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
    group_id='danawa-gpu-processor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# List of words to be removed from the model
words_to_remove = ['대원시티에스', '피씨디렉트', '디앤디컴', '제이씨현', '에즈윈', '대원씨티에스', '웨이코스', '마이크로닉스', '지포스', '라데온', 'D6X']

# Color translation dictionary
color_translation = {
    '핑크': 'Pink', '화이트': 'White', '블랙': 'Black', '레드': 'Red', 
    '블루': 'Blue', '그린': 'Green', '옐로우': 'Yellow', '퍼플': 'Purple', 
    '실버': 'Silver', '골드': 'Gold'
}

def extract_company_model_memory_color_rgb(row):
    parts = row.split()
    company = parts.pop(0)  # First word is the company
    memory = ""
    color = ""
    model_parts = []
    has_rgb = False

    for part in parts:
        if re.search(r'\d+GB', part):  # Check if part contains memory information
            memory = part.replace('GB', '').strip()
        elif part.lower() in color_translation.keys() or part.lower() in [v.lower() for v in color_translation.values()]:
            color = color_translation.get(part.lower(), part.capitalize())
        elif part.upper() == 'RGB':
            has_rgb = True
        elif part not in words_to_remove:
            model_parts.append(part)
    
    model = " ".join(model_parts).strip().upper()  # Remaining parts form the model name

    return company, model, memory, color, has_rgb

def clean_product_name_v2(product_name):
    cleaned_name = re.sub(r'\(.*?\)', '', product_name)  # Remove parentheses
    cleaned_name = re.sub(r'\d+GB', '', cleaned_name)  # Remove memory information
    cleaned_name = re.sub(r'\b(black|white|red|blue|green|yellow|purple|silver|gold|핑크|화이트|블랙|레드|블루|그린|옐로우|퍼플|실버|골드)\b', '', cleaned_name, flags=re.IGNORECASE)  # Remove color information
    cleaned_name = re.sub(r'\bRGB\b', '', cleaned_name, flags=re.IGNORECASE)  # Remove RGB
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip().upper()  # Remove extra spaces and convert to uppercase
    return cleaned_name

def process_data(danawa_gpu, standard_gpu):
    

    # Add chipset_type column based on productName containing '라데온'
    danawa_gpu['chipset_type'] = danawa_gpu['productName'].apply(lambda x: 'AMD' if '라데온' in x else 'NVIDIA')
    danawa_gpu['Type'] = "GPU"

    # Apply the function to the productName column
    danawa_gpu[['Company', 'Model', 'Memory', 'Color', 'RGB']] = danawa_gpu['productName'].apply(lambda x: pd.Series(extract_company_model_memory_color_rgb(x)))

    # Apply the function to clean the productName column
    danawa_gpu['productName'] = danawa_gpu['productName'].apply(clean_product_name_v2)

    # Remove rows where productName contains '해외'
    danawa_gpu = danawa_gpu[~danawa_gpu['productName'].str.contains('해외')]

    # Remove commas from productPriceStr column and rename it to Price
    danawa_gpu['Price'] = danawa_gpu['productPriceStr'].str.replace(',', '')
    danawa_gpu.drop(columns=['productPriceStr'], inplace=True)

    # Add ComponentID column
    danawa_gpu['ComponentID'] = danawa_gpu.apply(lambda x: f"{x['Type']}#{x['Company']}#{x['Model']}", axis=1)

    # Add Date column with current date and time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    danawa_gpu['Date'] = current_time

    # Add Shop column
    danawa_gpu['Shop'] = 'danawa'

    # Rename productUrlAll to URL
    danawa_gpu.rename(columns={'productUrlAll': 'URL'}, inplace=True)

    # Rename productImage to image_url and add https: if it starts with //
    danawa_gpu.rename(columns={'productImage': 'image_url'}, inplace=True)
    danawa_gpu['image_url'] = danawa_gpu['image_url'].apply(lambda x: 'https:' + x if x.startswith('//') else x)

    # Convert RGB column to boolean
    danawa_gpu['RGB'] = danawa_gpu['RGB'].astype(bool)

    # 중복 데이터 제거
    danawa_gpu.drop_duplicates(subset=['ComponentID', 'URL'], inplace=True)

    # Drop productId, productName, and productSpec columns
    danawa_gpu.drop(columns=['productId', 'productName', 'productSpec'], inplace=True)

    # Join danawa_gpu and standard_gpu on ComponentID and Memory
    final_gpu = pd.merge(danawa_gpu, standard_gpu, left_on=['ComponentID', 'Memory'], right_on=['ComponentID', '메모리'], how='inner')

    # Drop the columns that are no longer needed after join
    final_gpu.drop(columns=['메모리', '회사', '모델'], inplace=True)
    final_gpu.fillna('', inplace=True)

    # Extract necessary columns
    final_data = final_gpu[['ComponentID', 'Type', 'Date', 'Shop', 'Price', 'URL']].copy()
    final_json_data = final_data.to_dict(orient='records')
    
    return final_json_data

# RAM standard CSV 불러오기
standard_gpu_path = '/app/gpu_standard.csv'
standard_gpu = pd.read_csv(standard_gpu_path)
standard_gpu['ComponentID'] = 'GPU#' + standard_gpu['회사'] + '#' + standard_gpu['모델']
standard_gpu['메모리'] = standard_gpu['메모리'].astype(int).astype(str)

# Kafka 배치 처리
try:
    while True:
        raw_messages = consumer.poll(timeout_ms=3000)
        batch = []
        for tp, messages in raw_messages.items():
            for message in messages:
                raw_data = message.value
                batch.append(raw_data)

        if batch:
            data_df = pd.DataFrame(batch)
            processed_data = process_data(data_df, standard_gpu)
            if processed_data:
                for data in processed_data:
                    producer.send(PROCESSED_TOPIC, value=json.dumps(data))
                producer.flush()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
