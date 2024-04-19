"""
This is the HTTP function of salary project App2 which get job titles from table and send them to salary-queue1
"""

import azure.functions as func
import pyodbc
import os
import json 
from azure.storage.queue import QueueServiceClient
import base64
import logging

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
        error_message = f"Error sending message to the queue '{queue_name}': {str(e)}"
        logging.error(error_message)

def main(req: func.HttpRequest) -> func.HttpResponse:
    server = os.environ["DbServer"]
    database = os.environ["DbName"]
    username = os.environ["DbUsername"]
    password = os.environ["DbPassword"]
    table_name = os.environ["DbJobtitleTable"]

    # Construct the connection string
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    try:
        # Establish a database connection
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()
            query = f"SELECT JobTitles FROM {table_name}"
            
            # Execute the query
            cursor.execute(query)

            # Fetch all the results (first column of each row)
            all_job_titles = [row[0] for row in cursor.fetchall()]

            # Close the cursor
            cursor.close()

        # Define the chunk size
        chunk_size = 50

        # Iterate over the list in chunks and send each chunk to the queue
        for i in range(0, len(all_job_titles), chunk_size):
            chunk = all_job_titles[i:i + chunk_size]
            send_to_queue("salary-queue1", chunk)
            logging.info(f"Sent chunk to the queue: {chunk}")

        # Return the result as an HTTP response
        return func.HttpResponse(f"All job titles divided and sent to the salary-queue1.", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
