"""
This is the first queue trigger function of salary project App1 which get keys from queue
and scrape job titles using keys send them to next queue.
"""


# import logging
# import json
# import azure.functions as func
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# from bs4 import BeautifulSoup
# from selenium.webdriver.chrome.service import Service
# import os
# import time
# from azure.storage.queue import QueueServiceClient
# import base64



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

# def send_to_queue(queue_name, message):
#     connection_string = os.environ['AzureWebJobsStorage']
#     queue_service_client = QueueServiceClient.from_connection_string(connection_string)
#     queue_client = queue_service_client.get_queue_client(queue_name)

#     try:
#         # Convert the message to a JSON-encoded string
#         message_json = json.dumps(message)
#         message_base64 = base64.b64encode(message_json.encode('utf-8')).decode('utf-8')
#         queue_client.send_message(message_base64)
#         logging.info(f"Message has been sent to the '{queue_name}'.")

#     except Exception as e:
#         error_message = f"Error sending message to the '{queue_name}': {str(e)}"
#         logging.error(error_message)

# def scrape_jobtitles(url, driver, key):
#     search_input = driver.find_element(By.ID, "txt_typeahead_globalsearchjob")
#     search_input.clear()
#     search_input.send_keys(key)

#     time.sleep(2)

#     page_source = driver.page_source
#     soup = BeautifulSoup(page_source, "html.parser")
#     scraped_job_titles_container = soup.find("div", class_="globalheader-job-typeahead-menu")
#     if scraped_job_titles_container:
#         scraped_job_titles = [title.text.strip() for title in scraped_job_titles_container.find_all("div", class_="typeahead-suggestion")]
#         # Remove duplicates from scraped_job_titles
#         scraped_job_titles = list(set(scraped_job_titles))
#         logging.info(f'scraped_data: {scraped_job_titles}')
#         # Add the scraped job titles to the database
#         for job_title in scraped_job_titles:
#             send_to_queue("salary-app1-queue2", [job_title])

#     else:
#         logging.info(f"No job titles found for key '{key}'")




# def main(msg: func.QueueMessage):
#     try:
#         message_body = msg.get_body().decode('utf-8')
#         keys = json.loads(message_body)



#         for key in keys:
#             logging.info(f"message in salary-app1-queue1 is : {key}")
#             url = 'https://www.salary.com/research/jobs'
#             # driver.get(url)
#             # time.sleep(30)
#             # scrape_jobtitles(url,driver,key)
#             # time.sleep(2)
#             # # driver.quit()
#             try:
#                 driver.get(url)
#                 time.sleep(30)
#                 scrape_jobtitles(url, driver, key)
#                 time.sleep(2)
#             except Exception as ex:
#                 logging.error(f"Error while processing key '{key}': {str(ex)}")
#                 # Add more specific exception handling here, e.g., for timeout errors
#             finally:
#                 driver.quit()

        
#     except Exception as ex:
#         logging.error(f"Error processing queue message: {str(ex)}")
# ========================

import logging
import json
import azure.functions as func
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
import os
import time
from azure.storage.queue import QueueServiceClient
import base64
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument(
"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
)
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--enable-javascript")
# chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
service = Service(ChromeDriverManager().install())
logging.info(f"Chrome Driver Installation Status: {service}")
driver = webdriver.Chrome(service=service, options=chrome_options)

def send_to_queue(queue_name, message):
    connection_string = os.environ['AzureWebJobsStorage']
    queue_service_client = QueueServiceClient.from_connection_string(connection_string)
    queue_client = queue_service_client.get_queue_client(queue_name)

    try:
        # Convert the message to a JSON-encoded string
        message_json = json.dumps(message)
        message_base64 = base64.b64encode(message_json.encode('utf-8')).decode('utf-8')
        queue_client.send_message(message_base64)
        logging.info(f"Message has been sent to the '{queue_name}'.")

    except Exception as e:
        error_message = f"Error sending message to the '{queue_name}': {str(e)}"
        logging.error(error_message)


def main(msg: func.QueueMessage):
    try:
        message_body = msg.get_body().decode('utf-8')
        keys = json.loads(message_body)
        

        # chrome_options = Options()
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument(
        #     "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        # )
        # chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--enable-javascript")
        # # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        # service = Service(ChromeDriverManager().install())
        # logging.info(f"Chrome Driver Installation Status: {service}")
        # driver = webdriver.Chrome(service=service, options=chrome_options)


        for key in keys:
            logging.info(f"message in salary-app1-queue1 is : {key}")
            url = 'https://www.salary.com/research/jobs'
           
            try:
                driver.get(url)
                time.sleep(20)
                search_input = driver.find_element(By.ID, "txt_typeahead_globalsearchjob")
                search_input.clear()
                search_input.send_keys(key)

                time.sleep(5)

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                scraped_job_titles_container = soup.find("div", class_="globalheader-job-typeahead-menu")
                if scraped_job_titles_container:
                    scraped_job_titles = [title.text.strip() for title in scraped_job_titles_container.find_all("div", class_="typeahead-suggestion")]
                    # Remove duplicates from scraped_job_titles
                    scraped_job_titles = list(set(scraped_job_titles))
                    logging.info(f'scraped_data: {scraped_job_titles}')
                    # Add the scraped job titles to the database
                    for job_title in scraped_job_titles:
                        send_to_queue("salary-app1-queue2", [job_title])

                else:
                    logging.info(f"No job titles found for key '{key}'")
                driver.quit()
            except Exception as ex:
                logging.error(f"Error while processing key '{key}': {str(ex)}")
               
        
    except Exception as ex:
        logging.error(f"Error processing queue message: {str(ex)}")