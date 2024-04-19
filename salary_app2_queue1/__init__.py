"""
This is the first queue trigger function of salary project of App2. It will generate links and add
view table at the end and send them to salary-queue2.
"""


import logging
import json
import azure.functions as func
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
from azure.storage.queue import QueueServiceClient
import os
import base64
import time
import requests
from urllib.parse import urljoin


def send_to_queue(queue_name, messages):
    connection_string = os.environ['AzureWebJobsStorage']
    queue_service_client = QueueServiceClient.from_connection_string(connection_string)
    queue_client = queue_service_client.get_queue_client(queue_name)

    try:
        for message in messages:
            message_list = [message]
            messages_json = json.dumps(message_list)
            message_base64 = base64.b64encode(messages_json.encode('utf-8')).decode('utf-8')
            queue_client.send_message(message_base64)
            logging.info(f"Job title '{message}' has been sent to the '{queue_name}'.")

    except Exception as e:
        error_message = f"Error sending message to the '{queue_name}': {str(e)}"
        logging.error(error_message)

def scrape_data(job_titles):

    base_url = "https://www.salary.com/tools/salary-calculator/search?keyword={}&location="
    job_urls = [base_url.format(job.replace(" ", "%20")) for job in job_titles]
    # Remove duplicates from job_urls
    job_urls = list(set(job_urls))

    results = []
    for job_url in job_urls:
        response = requests.get(job_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find all first divs
        first_divs = soup.find_all('div', class_='sal-popluar-skills margin-top20')

        # Loop through each first div
        for first_div in first_divs:
            # Find the second div within each first div
            second_div = first_div.find('div', class_='margin-bottom10 font-semibold sal-jobtitle')

            # Find the <a> tag within the second div
            a_tag = second_div.find('a', class_='a-color font-semibold margin-right10')

            # Get the href attribute from the <a> tag
            href_value = a_tag['href'] if a_tag else None

            # Append "?view=table" to the URL
            full_url = urljoin(job_url, href_value + "?view=table") if href_value else None
            results.append(full_url)
    # Remove duplicates from results
    results = list(set(results))
    logging.info(f"result: {results}")

    send_to_queue("salary-queue2", results)

        
   
def main(msg: func.QueueMessage):
    try:
        message_body = msg.get_body().decode('utf-8')
        job_titles = json.loads(message_body)

        # chrome_options = Options()
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument(
        #     "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        # )
        # chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--enable-javascript")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--disable-dev-shm-usage")

        # service = Service(ChromeDriverManager().install())
        # logging.info(f"Chrome Driver Installation Status: {service}")

        # driver = webdriver.Chrome(service=service, options=chrome_options)


      
        print(f"message in salary-queu1 is : {job_titles}")
        
        time.sleep(5)
        scrape_data(job_titles)
        time.sleep(2)

    except Exception as ex:
        logging.error(f"Error processing queue message: {str(ex)}")

