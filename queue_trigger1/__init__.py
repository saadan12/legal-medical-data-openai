"""
This code will get message from queue1 and scrape profile links and send them to queue2
"""
import azure.functions as func
from azure.storage.queue import QueueClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import base64
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import logging

def main(msg: func.QueueMessage) -> None:
    # Define Azure Storage connection string
    connection_string = ""

    input_queue_name = 'queue1'
    output_queue_name = 'queue2'

    try:

        message_content = msg.get_body().decode("utf-8")
        scraped_data = scrape_data_with_selenium(message_content)
        if scraped_data:
            # Split the scraped data into individual URLs
            scraped_urls = scraped_data.split('\n')

            # Initialize the QueueClient for the output queue (queue2)
            output_queue_client = QueueClient.from_connection_string(connection_string, output_queue_name)

            # Enqueue each URL as a separate message to the output queue (queue2)
            for url in scraped_urls:
                if url:
                    encoded_url = base64.b64encode(url.encode()).decode()
                    output_queue_client.send_message(encoded_url)
                    print(f"Scraped URL {url}, encoded, and sent to {output_queue_name}\n")

        else:
            print(f"No data scraped from {message_content}")

    except Exception as e:
        print(f"Error processing message: {str(e)}")

def scrape_data_with_selenium(url):
    driver = None  

    try:
        # options = webdriver.ChromeOptions()
        # options.add_argument("--headless")  # Run headless (without a graphical user interface)
        # driver = webdriver.Chrome(options=options) 
        chrome_options = Options()

        chrome_options.add_argument("--no-sandbox")

        chrome_options.add_argument(

            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"

        )

        chrome_options.add_argument("--disable-extensions")

        chrome_options.add_argument("--enable-javascript")

        chrome_options.add_argument("--headless")

        chrome_options.add_argument("--disable-dev-shm-usage")

        

        service = Service(ChromeDriverManager().install())

        logging.info(f"Chrome Driver Installation Status : {service}")

        driver = webdriver.Chrome(service=service,options=chrome_options)

        driver.get(url)
        view_profile_links = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.view-profile-btn'))
        )

        # If there are no "View Profile" links on the current page, stop scraping
        if not view_profile_links:
            return ""

        hrefs = [link.get_attribute("href") for link in view_profile_links]
        return "\n".join(hrefs)

    except Exception as e:
        return f"Error while scraping data with Selenium for URL: {url}\n{str(e)}"

    finally:
        if driver:
            driver.quit()

# ===============================
# import logging

# import azure.functions as func


# def main(msg: func.QueueMessage) -> None:
#     logging.info(f'Python queue trigger function processed a queue item: %s',
#                  msg.get_body().decode('utf-8'))
