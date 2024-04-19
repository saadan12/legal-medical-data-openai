
import azure.functions as func
from azure.storage.queue import QueueClient
import requests
import base64
from bs4 import BeautifulSoup
import logging
import os
import json


def main(msg: func.QueueMessage) -> None:
    # Define Azure Storage connection string
    connection_string = os.environ['AzureWebJobsStorage']
    output_queue_name = 'smc-queue2'

    try:
        message_content = msg.get_body().decode("utf-8")
        urls = json.loads(message_content)
        logging.info(f"message in queue1: {urls}")

        # Initialize the QueueClient for the output queue (queue2)
        output_queue_client = QueueClient.from_connection_string(connection_string, output_queue_name)

        for url in urls:
            scraped_data = scrape_data_with_selenium(url)
            if scraped_data:
                try:
                    # Attempt to get the queue properties; this will throw an exception if the queue doesn't exist
                    output_queue_client.get_queue_properties()
                except Exception:
                    # The queue does not exist, so create it
                    output_queue_client.create_queue()
                    logging.info(f"Queue '{output_queue_name}' has been created.")

                # Send the scraped data as a separate message to the output queue (queue2)
                encoded_data = base64.b64encode(json.dumps(scraped_data).encode()).decode()
                output_queue_client.send_message(encoded_data)
                logging.info(f"Scraped data has been encoded and sent to {output_queue_name}\n")

                # Print the scraped data
                logging.info("Scraped Data:")
                for data in scraped_data:
                    logging.info(data)
            else: 
                 # If no scraped data, send the URL as is to "queue2"
                url_list = [url]  # Create a list containing the URL
                encoded_urls = base64.b64encode(json.dumps(url_list).encode()).decode()
                output_queue_client.send_message(encoded_urls)
                logging.info(f"URL '{url}' has been sent to {output_queue_name}\n")

    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")

def scrape_data_with_selenium(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    base_url = "https://www.gao.gov"  # Base URL to prepen

    # Find all h4 tags with the specified class
    h4_tags = soup.find_all('h4', class_='c-search-result__header gao-adv-search-card')

    # List to store href values
    hrefs = []

    # Loop through each h4 tag and get the href attribute from the enclosed anchor tag
    for h4_tag in h4_tags:
        anchor_tag = h4_tag.find('a')
        if anchor_tag:
            href_value = anchor_tag.get('href')
            complete_url = f"{base_url}{href_value}"
            hrefs.append(complete_url)

    return hrefs

   



    