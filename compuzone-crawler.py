from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ProcessPoolExecutor
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import random
import csv
import os
import json
import sys
import requests

# 크롬 드라이버 경로 설정
#CHROMEDRIVER_PATH = '/app/chromedriver-linux'
#CHROME_BINARY_PATH = '/usr/bin/google-chrome'

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36 Edg/99.0.1150.46"
]

date_str = time.strftime("%Y-%m-%d %H-%M-%S")

def random_sleep():
    time.sleep(random.uniform(1, 3))

def setup_driver():
    
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
    
    # chrome_options = webdriver.ChromeOptions()
    # user_agent = random.choice(user_agents)
    # chrome_options.binary_location = CHROME_BINARY_PATH
    # chrome_options.add_argument(f'user-agent={user_agent}')
    # chrome_options.add_argument('--headless')  # 필요시 주석 해제
    # chrome_options.add_argument('--window-size=1920,1080')
    # chrome_options.add_argument('--start-maximized')
    # chrome_options.add_argument('--disable-dev-shm-usage') # 공유 메모리 사용해제
    # chrome_options.add_argument('--disable-gpu')  # 필요시 주석 해제
    # chrome_options.add_argument('--no-sandbox')
    # chrome_options.add_argument('lang=ko=KR')
    # chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    # chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    # chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # driver = webdriver.Chrome(service=ChromeService(executable_path=CHROMEDRIVER_PATH), options=chrome_options)
    # driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    # return driver
    return browser

def scroll_to_bottom(driver):
    scroll_location = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        random_sleep()
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        if scroll_location == scroll_height:
            break
        else:
            scroll_location = scroll_height

def get_page_count(driver):
    scroll_to_bottom(driver)
    page_cnt = driver.find_element(By.XPATH, '//*[@id="page_wrap"]/div')
    return int(page_cnt.text[::-1].split('\n')[0])

def get_product_data(driver):
    product_data = []
    list_element = driver.find_element(By.XPATH, '//*[@id="product_list_ul"]')
    list_cnt = list_element.find_elements(By.CLASS_NAME, 'CProductDivNo')
    for i in range(1, len(list_cnt) + 1):
        product_xpath = f'//*[@id="product_list_ul"]/li[{i}]/div[2]/div[2]/div[1]/a'
        price_xpath = f'//*[@id="product_list_ul"]/li[{i}]/div[2]/div[3]/div[1]/a/div/span/strong'
        img_frame_xpath = f'//*[@id="product_list_ul"]/li[{i}]/div[2]/div[1]'
        spec_xpath = f'//*[@id="product_list_ul"]/li[{i}]/div[2]/div[2]/div[2]'
        img_frame = driver.find_element(By.XPATH, img_frame_xpath)
        img = img_frame.find_element(By.TAG_NAME, 'img').get_attribute('src')
        product = driver.find_element(By.XPATH, product_xpath)
        link = product.get_attribute('href')
        spec = driver.find_element(By.XPATH, spec_xpath).text.strip()
        try:
            price = driver.find_element(By.XPATH, price_xpath).text.replace(',', '')
        except NoSuchElementException:
            price = '재입고 예정'
        product_data.append((product.text.replace(',', ''), product.text.split(']')[0][1:], spec, img, price, link))
    return product_data

def send_to_kafka_batch(producer, topic, messages):
    for message in messages:
        producer.send(topic, value=message.encode('utf-8'))
    producer.flush()

def create_kafka_topic(topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=[
            'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
            'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
            'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
        ],
        client_id='crawler-compuzone'
    )
    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic {topic_name} created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic {topic_name} already exists.")

def scrape_data(url_title_method):
    url, title, method = url_title_method
    driver = setup_driver()
    driver.get(url)
    random_sleep()
    total_pages = get_page_count(driver)
    all_product_data = []

    for j in range(1, total_pages + 1):
        scroll_to_bottom(driver)
        all_product_data.extend(get_product_data(driver))
        next_url = f'{url}&StartNum={j}00&PageNum={j}'
        driver.get(next_url)
        random_sleep()

    #output_dir = "compuzon"
    #os.makedirs(output_dir, exist_ok=True)
    
    kafka_topic = f"raw-data.compuzone.{title}.json"
    create_kafka_topic(kafka_topic)

    producer = KafkaProducer(bootstrap_servers=[
        'kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092',
        'kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092',
        'kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092'
    ])
    
    product_data_batch = [
        json.dumps({
            "title": title,
            "name": product[0],
            "company": product[1],
            "spec": product[2],
            "img": product[3],
            "price": product[4],
            "link": product[5]
        }) for product in all_product_data
    ]
    send_to_kafka_batch(producer, kafka_topic, product_data_batch)
    
    #save_to_csv(f'{output_dir}/{title}_{date_str}.csv', all_product_data, method)
    driver.quit()

def save_to_csv(filename, data, method):
    with open(filename, method, newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        if filename != 'case.csv':
            writer.writerow(["Product", "Company", "Spec", "img", "Price", "link"])
        writer.writerows(data)

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
    urls = [
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getPaging&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1012&DivNo=0&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=7&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1013&DivNo=0&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1014&DivNo=0&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1276&DivNo=0&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1148&DivNo=0&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getPaging&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1020&DivNo=2054&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=7&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=N&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1016&DivNo=0&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1147&DivNo=2750&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=N&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1147&DivNo=2751&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=N&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
        'https://www.compuzone.co.kr/product/product_list.htm?actype=getTotalPageCount&SelectProductNo=&orderlayerx=&orderlayery=&BigDivNo=4&MediumDivNo=1147&DivNo=2749&PageCount=100&StartNum=0&PageNum=1&PreOrder=recommand&lvm=L&hot_keyword=&left_menu_open=&ps_po=P&DetailBack=&CompareProductNoList=&CompareProductDivNo=&IsProductGroupView=&ScrollPage=2&ProductType=list&setPricechk=&MD_CopyCategory=N&BD_CopyCategory=N&OAuthCertChk=0&PageType=ProductList&splist_kw=&SchMinPrice=0&SchMaxPrice=0&select_page_cnt=100&BottomQuery=',
    ]

    titles = ['cpu', 'mainboard', 'ram', 'ssd', 'power', 'cooler', 'gpu', 'case', 'case', 'case']
    methods = ['w', 'w', 'w', 'w', 'w', 'w', 'w', 'a', 'a', 'a']

    url_title_method = list(zip(urls, titles, methods))

    # output_dir = "compuzon"
    # os.makedirs(output_dir, exist_ok=True)
    # case_csv_path = f'{output_dir}/case_{date_str}.csv'

    # with open(case_csv_path, 'w', newline='', encoding='utf8') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(["Product", "Company", "img", "Price", "link"])

    # Crawler 작업 시작 알림
    bot_name = "컴퓨존 Crawler"
    cur_date = time.strftime("%Y-%m-%d %H:%M:%S")
    title = (f"[Crawler] 컴퓨존({cur_date}) - Start")
    message = (f"컴퓨존 Crawler 시작...")
    send_slack_notification(bot_name, title, message)

    # Crawling 시작
    with ProcessPoolExecutor(max_workers=4) as executor:
       executor.map(scrape_data, url_title_method)
    
    end_time = time.time()  # End time
    total_execution = f"{end_time - start_time:.2f} seconds"
    
    # 작업 완료시 Slack 알림
    cur_date = time.strftime("%Y-%m-%d %H:%M:%S")
    title = (f"[Crawler] 컴퓨존({cur_date}) - End")
    message = (f"Crawler 완료 \n총 걸린시간 : {total_execution}")
    send_slack_notification(bot_name, title, message)

if __name__ == "__main__":
    main()
