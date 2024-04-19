"""
This is the second queue trigger function of salary project of App2.it will scrape links for all locations
and send them to salary-queue3.
"""

import azure.functions as func
from azure.storage.queue import QueueClient
import requests
import base64
from bs4 import BeautifulSoup
import logging
import os
import json
from urllib.parse import urljoin


def main(msg: func.QueueMessage) -> None:
    # Define Azure Storage connection string
    connection_string = os.environ['AzureWebJobsStorage']
    output_queue_name = 'salary-queue3'

    try:
        message_content = msg.get_body().decode("utf-8")
        urls = json.loads(message_content)
        logging.info(f"message in queue2: {urls}")

        # Initialize the QueueClient for the output queue (queue2)
        output_queue_client = QueueClient.from_connection_string(connection_string, output_queue_name)

        for url in urls:
            scraped_data = scrape_data(url)
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
                 # If no scraped data, send the URL as is to "queue3"
                url_list = [url]  # Create a list containing the URL
                encoded_urls = base64.b64encode(json.dumps(url_list).encode()).decode()
                output_queue_client.send_message(encoded_urls)
                logging.info(f"URL '{url}' has been sent to {output_queue_name}\n")

    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")

def scrape_data(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        country_div = soup.find('div', class_='sal-national-flag-flex')
        country_links = country_div.find_all('a', class_='font-semibold', href=True)

        country_list = []
        base_url = "https://www.salary.com"

        # Loop through all countries and print text and href
        for country_link in country_links:
            country_text = country_link.text.strip()
            country_href = country_link['href']
            absolute_url = urljoin(base_url, country_href)

            # Append "?view=table" to the URL
            absolute_url += "?view=table"
            country_list.append(absolute_url)

        return country_list
    except:
        return [url]

   



    
    