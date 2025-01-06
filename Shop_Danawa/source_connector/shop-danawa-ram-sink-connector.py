import json
import time
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error

# Kafka 설정
KAFKA_BROKERS = [
    'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
    'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
    'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
]
PROCESSED_TOPIC = 'processed-data.shop-danawa.ram.json'

# MySQL 설정
MYSQL_HOST = '43.202.109.230'
MYSQL_DATABASE = 'hdh'
MYSQL_USER = 'hdh'
MYSQL_PASSWORD = 'spoid123#'

# Kafka Consumer 생성
consumer = KafkaConsumer(
    PROCESSED_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='mysql-sink-connector',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def save_to_mysql(records):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
            INSERT INTO Price (ComponentID, Type, Date, Shop, Price, URL) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Price=VALUES(Price), URL=VALUES(URL)
            """
            cursor.executemany(insert_query, records)
            connection.commit()
            cursor.close()
            connection.close()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

while True:
    raw_messages = consumer.poll(timeout_ms=3000)
    records = []
    for tp, messages in raw_messages.items():
        for message in messages:
            #data = message.value
            data = json.loads(message.value)
            record = (
                data['ComponentID'],
                data['Type'],
                data['Date'],
                data['Shop'],
                data['Price'],
                data['URL']
            )
            records.append(record)
    if records:
        save_to_mysql(records)
