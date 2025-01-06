# -*- coding: utf-8 -*-

import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import json
import sys
import requests
from datetime import datetime, timedelta
from pytz import timezone
import csv
import os
import shutil
import traceback
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from concurrent.futures import ProcessPoolExecutor

PROCESS_COUNT = 4

CRAWLING_DATA_CSV_FILE = 'CrawlingCategory.csv'
DATA_PATH = 'crawl_data'
DATA_REFRESH_PATH = f'{DATA_PATH}/Last_Data'

TIMEZONE = 'Asia/Seoul'

DATA_DIVIDER = '---'
DATA_REMARK = '//'
DATA_ROW_DIVIDER = '_'
DATA_PRODUCT_DIVIDER = '|'

STR_NAME = 'name'
STR_URL = 'url'
STR_CRAWLING_PAGE_SIZE = 'crawlingPageSize'

# 크롬 드라이버 경로 설정
CHROMEDRIVER_PATH = '/app/chromedriver-linux'
CHROME_BINARY_PATH = '/usr/bin/google-chrome'


class DanawaCrawler:
    def __init__(self):
        self.errorList = list()
        self.crawlingCategory = list()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36 Edg/99.0.1150.46"
        ]
        with open(CRAWLING_DATA_CSV_FILE, 'r', newline='') as file:
            for crawlingValues in csv.reader(file, skipinitialspace=True):
                if not crawlingValues[0].startswith(DATA_REMARK):
                    self.crawlingCategory.append({STR_NAME: crawlingValues[0], STR_URL: crawlingValues[1], STR_CRAWLING_PAGE_SIZE: int(crawlingValues[2])})

    def StartCrawling(self):
        self.chrome_option = webdriver.ChromeOptions()
        self.user_agent = random.choice(self.user_agents)
        self.chrome_option.binary_location = CHROME_BINARY_PATH
        self.chrome_option.add_argument(f'user-agent={self.user_agent}')
        self.chrome_option.add_argument('--headless')
        self.chrome_option.add_argument('--window-size=1920,1080')
        self.chrome_option.add_argument('--start-maximized')
        self.chrome_option.add_argument('--disable-dev-shm-usage')
        self.chrome_option.add_argument('--disable-gpu')
        self.chrome_option.add_argument('--no-sandbox')
        self.chrome_option.add_argument('lang=ko=KR')
        self.chrome_option.add_argument('--disable-blink-features=AutomationControlled')
        self.chrome_option.add_argument('--disable-software-rasterizer')
        self.chrome_option.add_argument('--enable-logging')
        self.chrome_option.add_argument('--v=1')
        self.chrome_option.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.chrome_option.add_experimental_option('useAutomationExtension', False)

        with ProcessPoolExecutor(max_workers=PROCESS_COUNT) as executor:
            executor.map(self.CrawlingCategory, self.crawlingCategory)

    def random_sleep(self):
        time.sleep(random.uniform(1, 2))

    def CrawlingCategory(self, categoryValue):
        crawlingName = categoryValue[STR_NAME]
        crawlingURL = categoryValue[STR_URL]
        crawlingSize = categoryValue[STR_CRAWLING_PAGE_SIZE]

        print('Crawling Start : ' + crawlingName)

        date_str = self.GetCurrentDate().strftime('%Y-%m-%d %H-%M-%S')
        #output_dir = "danawa"
        #os.makedirs(output_dir, exist_ok=True)
        #crawlingFile = open(f'{output_dir}/{crawlingName}_{date_str}.csv', 'w', newline='', encoding='utf8')
        #crawlingData_csvWriter = csv.writer(crawlingFile)
        #crawlingData_csvWriter.writerow([self.GetCurrentDate().strftime('%Y-%m-%d %H:%M:%S')])

        kafka_topic = f"raw-data.danawa.{crawlingName}.json"
        self.create_kafka_topic(kafka_topic)
        producer = KafkaProducer(bootstrap_servers=[
            'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
            'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
            'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
        ])

        try:
            browser = webdriver.Chrome(service=ChromeService(executable_path=CHROMEDRIVER_PATH), options=self.chrome_option)
            browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            browser.implicitly_wait(3)
            browser.get(crawlingURL)

            browser.find_element(By.XPATH, '//option[@value="90"]').click()

            wait = WebDriverWait(browser, 3)
            wait.until(EC.invisibility_of_element((By.CLASS_NAME, 'product_list_cover')))

            for i in range(-1, crawlingSize):
                if i == -1:
                    browser.find_element(By.XPATH, '//li[@data-sort-method="NEW"]').click()
                elif i == 0:
                    browser.find_element(By.XPATH, '//li[@data-sort-method="BEST"]').click()
                elif i > 0:
                    if i % 10 == 0:
                        try:
                            next_button = browser.find_element(By.XPATH, '//a[@class="edge_nav nav_next"]')
                            next_button.click()
                        except ElementClickInterceptedException:
                            print("ElementClickInterceptedException 발생, JavascriptExecutor를 사용하여 클릭")
                            browser.execute_script("arguments[0].click();", next_button)
                    else:
                        try:
                            page_button = browser.find_element(By.XPATH, f'//a[@class="num "][{i % 10}]')
                            page_button.click()
                        except NoSuchElementException:
                            print(f"NoSuchElementException 발생, 페이지 번호 {i % 10}가 존재하지 않음. 다음 제품으로 넘어갑니다.")
                            continue
                        except ElementClickInterceptedException:
                            print("ElementClickInterceptedException 발생, JavascriptExecutor를 사용하여 클릭")
                            browser.execute_script("arguments[0].click();", page_button)
                wait.until(EC.invisibility_of_element((By.CLASS_NAME, 'product_list_cover')))

                productListDiv = browser.find_element(By.XPATH, '//div[@class="main_prodlist main_prodlist_list"]')
                products = productListDiv.find_elements(By.XPATH, '//ul[@class="product_list"]/li')

                for product in products:
                    if not product.get_attribute('id'):
                        continue

                    if 'prod_ad_item' in product.get_attribute('class').split(' '):
                        continue
                    if product.get_attribute('id').strip().startswith('ad'):
                        continue

                    productId = product.get_attribute('id')[11:]
                    productName = product.find_element(By.XPATH, './div/div[2]/p/a').text.strip()
                    productPrices = product.find_elements(By.XPATH, './div/div[3]/ul/li')
                    productPriceStr = ''

                    productSpec = product.find_element(By.XPATH, './div/div[2]/dl/dd').get_attribute('innerText').strip()
                    productSpec = productSpec.replace('\n', ' ')

                    imgElement = product.find_element(By.XPATH, './div/div[1]/a[1]/img')
                    productImage = imgElement.get_attribute('data-original') if imgElement.get_attribute('data-original') else imgElement.get_attribute('src')

                    productUrlTag = product.find_element(By.XPATH, './div/div[2]/p/a')
                    productUrlAll = productUrlTag.get_attribute('href')

                    isMall = 'prod_top5' in product.find_element(By.XPATH, './div/div[3]').get_attribute('class')

                    if isMall:
                        for productPrice in productPrices:
                            if 'top5_button' in productPrice.get_attribute('class'):
                                continue

                            if productPriceStr:
                                productPriceStr += DATA_PRODUCT_DIVIDER

                            mallName = productPrice.find_element(By.XPATH, './a/div[1]').text.strip()
                            if not mallName:
                                mallName = productPrice.find_element(By.XPATH, './a/div[1]/span[1]').text.strip()

                            price = productPrice.find_element(By.XPATH, './a/div[2]/em').text.strip()

                            productPriceStr += f'{mallName}{DATA_ROW_DIVIDER}{price}'
                    else:
                        for productPrice in productPrices:
                            if productPriceStr:
                                productPriceStr += DATA_PRODUCT_DIVIDER

                            productType = productPrice.find_element(By.XPATH, './div/p').text.strip()
                            productType = productType.replace('\n', DATA_ROW_DIVIDER)
                            productType = self.RemoveRankText(productType)

                            price = productPrice.find_element(By.XPATH, './p[2]/a/strong').text.strip()

                            if productType:
                                productPriceStr += f'{productType}{DATA_ROW_DIVIDER}{price}'
                            else:
                                productPriceStr += f'{price}'

                    #crawlingData_csvWriter.writerow([productId, productName, productPriceStr, productSpec, productImage, productUrlAll])
                    product_data = {
                        "productId": productId,
                        "productName": productName,
                        "productPriceStr": productPriceStr,
                        "productSpec": productSpec,
                        "productImage": productImage,
                        "productUrlAll": productUrlAll
                    }
                    self.send_to_kafka(producer, kafka_topic, product_data)

        except Exception as e:
            print('Error - ' + crawlingName + ' ->')
            print(traceback.format_exc())
            self.errorList.append(crawlingName)

        #crawlingFile.close()
        browser.quit()

        print('Crawling Finish : ' + crawlingName)

    def send_to_kafka(self, producer, topic, data):
        try:
            producer.send(topic, value=json.dumps(data).encode('utf-8'))
            producer.flush()
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")

    def create_kafka_topic(self, topic_name):
        admin_client = KafkaAdminClient(
            bootstrap_servers=[
                'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
                'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
                'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
            ],
            client_id='crawler-danawa'
        )
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic {topic_name} created successfully.")
        except TopicAlreadyExistsError:
            print(f"Topic {topic_name} already exists.")

    def RemoveRankText(self, productText):
        if len(productText) < 2:
            return productText
        
        char1 = productText[0]
        char2 = productText[1]

        if char1.isdigit() and (1 <= int(char1) and int(char1) <= 9):
            if char2 == '위':
                return productText[2:].strip()
        
        return productText

    
    def GetCurrentDate(self):
        tz = timezone(TIMEZONE)
        return datetime.now(tz)
    
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


if __name__ == '__main__':
    crawler = DanawaCrawler()
        
    bot_name = "다나와 Crawler"
    cur_date = time.strftime("%Y-%m-%d %H:%M:%S")
    title = (f"[Crawler] 다나와({cur_date}) - Start")
    message = (f"다나와 Crawler 시작...")
    send_slack_notification(bot_name, title, message)
    
    start_time = time.time()
    crawler.StartCrawling()
    end_time = time.time()
    total_execution = f"{end_time - start_time:.2f} seconds"
    
    cur_date = time.strftime("%Y-%m-%d %H:%M:%S")
    title = (f"[Crawler] 다나와({cur_date}) - End")
    message = (f"Crawler 완료 \n총 걸린시간 : {total_execution}")
    send_slack_notification(bot_name, title, message)
