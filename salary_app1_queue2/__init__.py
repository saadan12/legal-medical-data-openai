"""
This is the second queue trigger function of salary project App1 which get job-titles from queue
and scrape job titles and send them to database table.
"""


import logging
import json
import azure.functions as func
import os
import time
import requests
import pyodbc
import re
from bs4 import BeautifulSoup

server = os.environ["DbServer"]
database = os.environ["DbName"]
username = os.environ["DbUsername"]
password = os.environ["DbPassword"]
table_name = os.environ["DbJobtitleTable"]

def scrape_jobtitles(job_titles):
    for job_title in job_titles:
        scraped_data = {"JobTitles": job_title}
        logging.info("First_Scraped_data:", scraped_data)
        send_data_to_database(scraped_data)

    base_url = "https://www.salary.com/job/searchresults?jobtitle={}"
    job_urls = [base_url.format(job.replace(" ", "%20")) for job in job_titles]

    for url in job_urls:
        response = requests.get(url,timeout=60)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the last page link
        last_page_link_element = soup.select_one('ul.pagination li:last-child a')

        page_links = []

        # Check if last_page_link_element is not None
        if last_page_link_element:
            # Extract the 'pg' parameter from the href attribute using regular expression
            match = re.search(r'pg=(\d+)', last_page_link_element['href'])

            # Get the matched numeric characters
            last_page = int(match.group(1))
            base_url = "https://www.salary.com"

            # Generate URLs for each page from the first page to the last page
            for page_number in range(1, last_page + 1):
                page_links.append(f"{base_url}{last_page_link_element['href'].replace(match.group(1), str(page_number))}")

            logging.info("Generated page URLs:")
            for link in page_links:
                response = requests.get(link)
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find all job titles within the <a> tags
                next_scraped_job_titles = [a.get_text(strip=True) for a in soup.find_all('a', class_='font-bold padding-left5')]
                next_scraped_job_titles = list(set(next_scraped_job_titles))

                # Add the scraped job titles to the database
                for job_title in next_scraped_job_titles:
                    scraped_data = {"JobTitles": job_title}
                    logging.info("Next_Scraped_data:", scraped_data)
                    send_data_to_database(scraped_data)

        else:
            logging.info("last_page_link_element is None.")
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all job titles within the <a> tags
            next_scraped_job_titles = [a.get_text(strip=True) for a in soup.find_all('a', class_='font-bold padding-left5')]
            next_scraped_job_titles = list(set(next_scraped_job_titles))

            # Add the scraped job titles to the database
            for job_title in next_scraped_job_titles:
                scraped_data = {"JobTitles": job_title}
                logging.info("Next_Scraped_data:", scraped_data)
                send_data_to_database(scraped_data)

def send_data_to_database(data):
    try:
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        if data is not None:
            # Check if the data already exists in the database
            select_sql = f"SELECT 1 FROM {table_name} WHERE JobTitles = ?"
            cursor.execute(
                select_sql,
                (
                    data["JobTitles"]
                ),
            )

            existing_data = cursor.fetchone()

            if existing_data:
                logging.info("Job Title already exists in the database. Skipping insertion.")
            else:
                # Insert the scraped data into the database
                insert_sql = f"INSERT INTO {table_name} (JobTitles) " \
                             "VALUES (?)"

                try:
                    cursor.execute(
                        insert_sql,
                        (
                            data["JobTitles"]
                        ),
                    )

                    conn.commit()
                    logging.info("Job Title uploaded to the table successfully.")

                except Exception as e:
                    error_message = f"Error sending data to the database for URL: {str(e)}"
                    logging.error(error_message)
        else:
            logging.error("Scraping data for URL resulted in None. Data not inserted into the database.")

    except Exception as e:
        error_message = f"Error sending data to the database for URL: {str(e)}"
        logging.error(error_message)

def main(msg: func.QueueMessage):
    try:
        message_body = msg.get_body().decode('utf-8')
        job_titles = json.loads(message_body)

        time.sleep(5)
        scrape_jobtitles(job_titles)
        time.sleep(2)

    except Exception as ex:
        logging.error(f"Error processing queue message: {str(ex)}")
