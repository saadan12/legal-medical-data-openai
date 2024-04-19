
import json
import logging
import base64
from azure.storage.queue import QueueServiceClient
import os
from urllib.parse import urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
import azure.functions as func

def scrape_total_pages(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the link with 'aria-label="Last page"'
    last_page_link = soup.find('a', {'aria-label': 'Last page'})

    # Extract the 'href' attribute and the text
    if last_page_link:
        href = last_page_link.get('href')
        print(f"last link is: {href}")

        last_page_text = last_page_link.get_text(strip=True)
        print(f"Text last page : {last_page_text}")

        page_number = href.split('page=')[-1]
        total_pages = int(page_number)+1
        print(f"total pages", total_pages)
        
    return total_pages


def increment_page_num(url, current_page, total_pages):
    page_urls = []

    current_page = int(current_page)  # Convert current_page to an integer
    total_pages = int(total_pages)  # Convert total_pages to an integer

    url_parts = url.split("&")
    for i in range(current_page, total_pages):
        # Find and update the 'page' parameter
        for j, part in enumerate(url_parts):
            if 'page=' in part:
                url_parts[j] = f'page={i}'

        # Recreate the URL
        modified_url = "&".join(url_parts)
        page_urls.append(modified_url)
   

    return page_urls

def send_to_queue(queue_name, messages):
    connection_string = os.environ['AzureWebJobsStorage']
    queue_service_client = QueueServiceClient.from_connection_string(connection_string)
    queue_client = queue_service_client.get_queue_client(queue_name)

    try:
        for message in messages:
            # Create a list with a single message and send it to the queue
            message_list = [message]
            messages_json = json.dumps(message_list)
            message_base64 = base64.b64encode(messages_json.encode('utf-8')).decode('utf-8')
            queue_client.send_message(message_base64)
            logging.info(f"URL '{message}' has been sent to the '{queue_name}'.")

    except Exception as e:
        error_message = f"Error sending message to the queue '{queue_name}': {str(e)}"
        logging.error(error_message)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        url = "https://www.gao.gov/gao-advanced-search?f%5B0%5D=by_content_type_advanced%3ABid%20Protest&f%5B1%5D=by_content_type_advanced%3ABid%20Protest%20Decision&page=0"

        if not url:
            return func.HttpResponse(
                "URL parameter is missing or empty. Please provide a valid 'url' parameter in the request body.",
                status_code=400
            )

        

        # Check if the 'pageNum' parameter exists in the URL
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        if 'page' in query_params:
            # Scrape the total page number using Selenium
            total_pages = scrape_total_pages(url)
            current_page = query_params['page'][0]
            page_urls = increment_page_num(url, current_page, total_pages)
            queue_name = "smc-queue1"  # Queue for modified URLs
        else:
            # If 'pageNum' does not exist, send the input URL directly to 'queue1'
            page_urls = [url]
            queue_name = "smc-queue1"  # Queue for unmodified URLs

        logging.info(f"{len(page_urls)} URLs have been generated for queue '{queue_name}' from URL: {url}")
        send_to_queue(queue_name, page_urls)

        result = {
            "message": "All URLs sent to the queue successfully."
        }
        return func.HttpResponse(json.dumps(result), status_code=200)

    except Exception as ex:
        logging.error(f"Error: {ex}")
        return func.HttpResponse(f"Internal Server Error: {ex}", status_code=500)
    




