"""
This is the third queue trigger function of salary project of App2. It will sscrape all data like salary, percentile
and others and send to database table.
"""   

import azure.functions as func
import logging
import json
from bs4 import BeautifulSoup
import os
import pyodbc
import requests


def clean_string(input_str):
    try:
        # Try to encode the string using utf-8, replace any errors with a hyphen
        cleaned_str = input_str.encode('utf-8', 'replace').decode('utf-8')
        return cleaned_str.replace('\u20b9', '-')  
    except Exception as e:
        logging.warning(f"Error cleaning string: {str(e)}")
        return input_str.replace('\u0142', '-') 

def extract_salary_value(salary_str):
    try:
        # Extract the numeric part of the salary string (remove currency symbol)
        salary_value = ''.join(char for char in salary_str if char.isdigit() or char == ',')
        
        # Extract the currency symbol
        currency_symbol = ''.join(char for char in salary_str if not char.isdigit() and char != ',')

        return salary_value, currency_symbol
    except Exception as e:
        logging.warning(f"Error extracting salary value: {str(e)}")
        return salary_str, None

def main(msg: func.QueueMessage) -> None:
    server = os.environ["DbServer"]
    database = os.environ["DbName"]
    username = os.environ["DbUsername"]
    password = os.environ["DbPassword"]
    table_name = os.environ["DbTableName1"]
    
    try:
        message_content = msg.get_body().decode("utf-8")
        message_list = json.loads(message_content)

        if message_list:
            for url in message_list:
                # logging.info(f"url is: {url}")
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')

                table = soup.find('table', class_='table-chart')
                rows = table.find('tbody').find_all('tr')

                # Extract values from each row and process one by one
                for row in rows:
                    values = [value.text.strip() for value in row.find_all('td')]
                    percentile, salary, location, last_updated = values
                    
                    # Process each item as needed
                    cleaned_salary, currency = extract_salary_value(salary)

                    scraped_data = {
                        "Percentile": percentile,
                        "Salary": cleaned_salary,
                        "Currency": currency,
                        "Location": location,
                        "LastUpdated": last_updated
                    }
                    print(scraped_data)
                    
                    send_data_to_database(scraped_data, server, database, username, password, table_name)
        
        else:
            logging.warning("No data found in the message list.")

    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")


def send_data_to_database(data, server, database, username, password, table_name):
    try:
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        if data is not None:
            # Check if the data already exists in the database
            select_sql = f"SELECT 1 FROM {table_name} WHERE Percentile = ? AND Salary = ? AND Currency = ? AND Location = ? AND LastUpdated = ?"
            cursor.execute(
                select_sql,
                (
                    data["Percentile"],
                    data["Salary"],
                    data["Currency"],
                    data["Location"],
                    data["LastUpdated"]
                ),
            )
            
            existing_data = cursor.fetchone()

            if existing_data:
                logging.info("Data already exists in the database. Skipping insertion.")
            else:
                # Insert the scraped data into the database
                insert_sql = f"INSERT INTO {table_name} (Percentile, Salary, Currency, Location, LastUpdated) " \
                             "VALUES (?, ? , ?, ?, ?)"

                try:
                    if data["Salary"] is not None:
                        cursor.execute(
                            insert_sql,
                            (
                                data["Percentile"],
                                data["Salary"],
                                data["Currency"],
                                data["Location"],
                                data["LastUpdated"]
                            ),
                        )

                        conn.commit()
                        logging.info("Data uploaded to the table successfully.")
                    else:
                        logging.error("Error extracting salary value. Data not inserted into the database.")

                except Exception as e:
                    error_message = f"Error sending data to the database for URL: {str(e)}"
                    logging.error(error_message)
                    logging.error(f"Actual Salary Value: {data['Salary']}")

        else:
            logging.error("Scraping data for URL resulted in None. Data not inserted into the database.")

    except Exception as e:
        error_message = f"Error sending data to the database for URL: {str(e)}"
        logging.error(error_message)
