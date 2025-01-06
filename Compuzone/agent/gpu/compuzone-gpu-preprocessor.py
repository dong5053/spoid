import pandas as pd
import re
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# Kafka 설정
RAW_TOPIC = 'raw-data.compuzone.gpu.json'
PROCESSED_TOPIC = 'processed-data.compuzone.gpu.json'

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
    group_id='compuzone-gpu-processor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# List of words to be removed from the model
words_to_remove = ['대원시티에스', '피씨디렉트', '디앤디컴', '제이씨현', '에즈윈', '대원씨티에스', '웨이코스', '마이크로닉스', '지포스', '라데온', 'D6X', 'RADEON', 'GEFORCE', 'RADEON™', 'D6']

# Color translation dictionary
color_translation = {
    '핑크': 'Pink', '화이트': 'White', '블랙': 'Black', '레드': 'Red', 
    '블루': 'Blue', '그린': 'Green', '옐로우': 'Yellow', '퍼플': 'Purple', 
    '실버': 'Silver', '골드': 'Gold'
}

def remove_square_brackets_and_stars(text):
    text = re.sub(r'\[.*?\]', '', text).strip()  # Remove content inside square brackets
    text = re.sub(r'★.*?★', '', text).strip()  # Remove content between stars and the stars themselves
    return text

def extract_company_model_memory_color_rgb(row):
    parts = row.split()
    company = ""
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

    return model, memory, color, has_rgb

def reorder_model(row):
    if row['Company'] == 'ASUS' and 'TUF GAMING' in row['Model']:
        parts = row['Model'].split(' ')
        tuf_index = parts.index('TUF')
        reordered_parts = parts[tuf_index:tuf_index+2] + parts[:tuf_index] + parts[tuf_index+2:]
        return ' '.join(reordered_parts)
    return row['Model']

def translate_company(company):
    translations = {
        'EMTEK': '이엠텍',
        'GALAX': '갤럭시',
        'LEADTEK': '리드텍'
    }
    return translations.get(company, company)

def process_data(compuzone_gpu, standard_gpu):
    # Apply the function to remove content inside square brackets
    compuzone_gpu['name'] = compuzone_gpu['name'].apply(remove_square_brackets_and_stars)

    # Convert specified columns to uppercase
    columns_to_uppercase = ['name', 'title', 'company']
    for column in columns_to_uppercase:
        compuzone_gpu[column] = compuzone_gpu[column].str.upper()

    # Add chipset_type column based on productName containing '라데온'
    compuzone_gpu['chipset_type'] = compuzone_gpu['name'].apply(lambda x: 'AMD' if 'RADEON' in x else 'NVIDIA')
    compuzone_gpu.rename(columns={'title': 'Type'}, inplace=True)
    
    # Company 컬럼값 한/영 변환
    compuzone_gpu['company'] = compuzone_gpu['company'].apply(translate_company)

    # Apply the function to the productName column
    compuzone_gpu[['Model', 'Memory', 'Color', 'RGB']] = compuzone_gpu['name'].apply(lambda x: pd.Series(extract_company_model_memory_color_rgb(x)))

    compuzone_gpu.rename(columns={'company': 'Company'}, inplace=True)

    # Apply the function to reorder the Model column
    compuzone_gpu['Model'] = compuzone_gpu.apply(reorder_model, axis=1)

    # Remove rows where productName contains '해외'
    compuzone_gpu = compuzone_gpu[~compuzone_gpu['name'].str.contains('해외')]

    # Remove commas from productPriceStr column and rename it to Price
    compuzone_gpu['Price'] = compuzone_gpu['price'].str.replace(',', '')
    compuzone_gpu.drop(columns=['price'], inplace=True)

    # Add ComponentID column
    compuzone_gpu['ComponentID'] = compuzone_gpu.apply(lambda x: f"{x['Type']}#{x['Company']}#{x['Model']}", axis=1)

    # Add Date column with current date and time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    compuzone_gpu['Date'] = current_time

    # Add Shop column
    compuzone_gpu['Shop'] = 'compuzone'

    # Rename productUrlAll to URL
    compuzone_gpu.rename(columns={'link': 'URL'}, inplace=True)

    # Rename productImage to image_url and add https: if it starts with //
    compuzone_gpu.rename(columns={'img': 'image_url'}, inplace=True)
    compuzone_gpu['image_url'] = compuzone_gpu['image_url'].apply(lambda x: 'https:' + x if x.startswith('//') else x)

    # Convert RGB column to boolean
    compuzone_gpu['RGB'] = compuzone_gpu['RGB'].astype(bool)

    # 중복 데이터 제거
    compuzone_gpu.drop_duplicates(subset=['ComponentID', 'URL'], inplace=True)

    # Drop spec and name columns
    compuzone_gpu.drop(columns=['spec', 'name'], inplace=True)

    # Join compuzone_gpu and standard_gpu on ComponentID and Memory
    final_gpu = pd.merge(compuzone_gpu, standard_gpu, left_on=['ComponentID', 'Memory'], right_on=['ComponentID', '메모리'], how='inner')

    # Drop the columns that are no longer needed after join
    final_gpu.drop(columns=['메모리', '회사', '모델'], inplace=True)
    final_gpu.fillna('', inplace=True)

    # Extract necessary columns
    final_data = final_gpu[['ComponentID', 'Type', 'Date', 'Shop', 'Price', 'URL']].copy()
    final_json_data = final_data.to_dict(orient='records')
    
    return final_json_data

# RAM standard CSV 불러오기
standard_gpu_path = 'gpu_standard.csv'
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
