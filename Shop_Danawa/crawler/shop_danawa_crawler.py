from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import csv
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import requests
import json
import sys
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# 크롬 드라이버 경로 설정
#service = ChromeService(ChromeDriverManager().install())
#CHROMEDRIVER_PATH = '/app/chromedriver-linux'
#CHROME_BINARY_PATH = '/usr/bin/google-chrome'
#service = ChromeService(executable_path=CHROMEDRIVER_PATH)


user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36 Edg/99.0.1150.46",
]


def send_to_kafka_batch(producer, topic, messages):
    for message in messages:
        producer.send(topic, value=message.encode('utf-8'))
    producer.flush()
    

# 토픽 생성 함수
def create_kafka_topic(topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=[
            'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
            'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
            'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
        ],
        client_id='crawler-shop-danawa'
    )
    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
    # admin_client.create_topics(new_topics=topic_list, validate_only=False)
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic {topic_name} created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic {topic_name} already exists.")


def random_sleep():
    time.sleep(random.uniform(1, 2))

def setup_browser():
    CHROMEDRIVER_PATH = '/app/chromedriver-linux'
    CHROME_BINARY_PATH = '/usr/bin/google-chrome'
    service = ChromeService(executable_path=CHROMEDRIVER_PATH)

    chrome_options = webdriver.ChromeOptions()
    user_agent = random.choice(user_agents)
    chrome_options.binary_location = CHROME_BINARY_PATH
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument('--headless')  # 필요시 주석 해제
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--disable-dev-shm-usage') # 공유 메모리 사용해제
    chrome_options.add_argument('--disable-gpu')  # 필요시 주석 해제
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('lang=ko=KR')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--enable-logging')
    chrome_options.add_argument('--v=1')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    print("Setting up Chrome browser")
    browser = webdriver.Chrome(service=service, options=chrome_options)
    browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    print("Chrome browser setup complete")
    return browser


def navigate_to_section(browser, section_xpath):
    browser.find_element(By.XPATH, section_xpath).click()
    random_sleep()

def get_product_info(browser):
    products = []
    rows = browser.find_elements(By.XPATH, '//*[@id="estimateMainProduct"]/div/div[2]/div[1]/table/tbody/tr')
    
    # rows = WebDriverWait(browser, 1).until(
    #     EC.presence_of_all_elements_located((By.XPATH, '//*[@id="estimateMainProduct"]/div/div[2]/div[1]/table/tbody/tr'))
    # )
    
    for row in rows:
        try:
            product_name = row.find_element(By.XPATH, './td[2]/p/a').text.strip()
            product_price = row.find_element(By.XPATH, './td[3]/p').text.strip()
            product_image_url = row.find_element(By.XPATH, './td[1]/div/img').get_attribute('src')
            product_spec = row.find_element(By.XPATH, './td[2]/div[1]/div/a').text.strip()
            onclick_value = row.find_element(By.XPATH, './td[2]/p/a').get_attribute('onclick')
            
            product_id = None
            if onclick_value:
                product_id = onclick_value.split('(')[1].split(',')[0]
            
            # https://shop.danawa.com/pc/?controller=estimateDeal&methods=productInformation&productSeq=28799654&marketPlaceSeq=16
            product_url = f"https://shop.danawa.com/pc/?controller=estimateDeal&methods=productInformation&productSeq={product_id}&marketPlaceSeq=16"
            
            products.append((product_name, product_spec, product_price, product_image_url, product_url))
        except Exception as e:
            print(f"Error parsing row: {e}")
    return products

def crawl_products(args):
    section_name, section_xpath = args
    browser = setup_browser()
    url = 'https://shop.danawa.com/virtualestimate/?controller=estimateMain&methods=index&marketPlaceSeq=16'
    browser.get(url)
    WebDriverWait(browser, 2).until(EC.presence_of_element_located((By.XPATH, '//*[@id="wish_product_list"]')))
    navigate_to_section(browser, section_xpath)
    products = []
    
    batch_size = 10  # 한 번에 전송할 데이터 개수
    # <데이터종류>.<사이트종류>.<부품종류>.<메시지타입>
    kafka_topic = f"raw-data.shop-danawa.{section_name}.json"
    
    create_kafka_topic(kafka_topic)
    
    producer = KafkaProducer(bootstrap_servers=[
        'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
        'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
        'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
    ])
    
    while True:
        for i in range(2, 12):  # 1~10 페이지 크롤링
            try:
                if i != 2:
                    browser.find_element(By.XPATH, f'//*[@id="estimateMainProduct"]/div/div[2]/div[2]/div/ul/li[{i}]/a').click()
                random_sleep()
                products.extend(get_product_info(browser))
                
                # 배치 전송
                if len(products) >= batch_size:
                    product_data_batch = [
                        json.dumps({
                            "section": section_name,
                            "name": product[0],
                            "spec": product[1],
                            "price": product[2],
                            "image_url": product[3],
                            "product_url": product[4]
                        }) for product in products
                    ]
                    send_to_kafka_batch(producer, kafka_topic, product_data_batch)
                    products.clear()
                
            except Exception as e:
                print(f"{i-1} 페이지가 존재하지 않습니다: {e}")
                break

        try:
            # "다음" 버튼 클릭
            next_button = browser.find_element(By.XPATH, '//*[@id="estimateMainProduct"]/div/div[2]/div[2]/div/ul/li[12]/a')
            next_button.click()
            random_sleep()
        except Exception as e:
            print(f"더 이상 페이지가 없습니다: {e}")
            break
        
    # 남은 데이터 전송
    if products:
        product_data_batch = [
            json.dumps({
                "section": section_name,
                "name": product[0],
                "spec": product[1],
                "price": product[2],
                "image_url": product[3],
                "product_url": product[4]
            }) for product in products
        ]
        send_to_kafka_batch(producer, kafka_topic, product_data_batch)
    
    
    browser.quit()
    producer.close()
    return section_name, products

def save_to_csv(section_name, products):
    date_str = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    directory = 'shop_danawa'
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_name = f"{directory}/{section_name}_{date_str}.csv"
    with open(file_name, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Product Name', 'Spec', 'Price', 'Image Url', 'Product Url'])
        writer.writerows(products)
    print(f"Saved {len(products)} products to {file_name}")

def send_slack_notification(bot_name, title, message):
    url = "https://hooks.slack.com/services/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"

    slack_data = {
        "username": bot_name,
        "icon_emoji": ":satellite:",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }

    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}

    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

def main():
    start_time = time.time()  # Start time

    sections = {
        'cpu': '//*[@id="wish_product_list"]/div/dl[1]/dd[1]',
        'cooler': '//*[@id="wish_product_list"]/div/dl[1]/dd[2]',
        'mainboard': '//*[@id="wish_product_list"]/div/dl[1]/dd[3]',
        'ram': '//*[@id="wish_product_list"]/div/dl[1]/dd[4]/a',
        'gpu': '//*[@id="wish_product_list"]/div/dl[1]/dd[5]/a',
        'storage': '//*[@id="wish_product_list"]/div/dl[1]/dd[6]/a',
        'case': '//*[@id="wish_product_list"]/div/dl[1]/dd[8]/a',
        'power': '//*[@id="wish_product_list"]/div/dl[1]/dd[9]/a'
    }

    # Crawler 작업 시작 알림
    bot_name = "샵다나와 Crawler"
    cur_date = time.strftime("%Y-%m-%d %H:%M:%S")
    title = (f"[Crawler] 샵다나와({cur_date}) - Start")
    message = (f"샵다나와 Crawler 시작...")
    send_slack_notification(bot_name, title, message)

    # Crawling 시작
    with ThreadPoolExecutor(max_workers = 4) as executor:
        results = list(executor.map(crawl_products, sections.items()))

    # for section_name, products in results:
    #     save_to_csv(section_name, products)

    end_time = time.time()  # End time
    total_execution = f"{end_time - start_time:.2f} seconds"

    # 작업 완료시 Slack 알림
    cur_date = time.strftime("%Y-%m-%d %H:%M:%S")
    title = (f"[Crawler] 샵다나와({cur_date}) - End")
    message = (f"Crawler 완료 \n총 걸린시간 : {total_execution}")
    send_slack_notification(bot_name, title, message)
    
    

if __name__ == "__main__":
    main()

