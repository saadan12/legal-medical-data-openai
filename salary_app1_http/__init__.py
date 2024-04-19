"""
This is the HTTP function of salary project App1 in which we are sending alphabetic keys
and combination of keys to salary-jobtitle-keys queue.
"""

import azure.functions as func
from azure.storage.queue import QueueServiceClient
import os
import json
import base64
import logging
import itertools

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

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        alphabet_list = list("abcdefghijklmnopqrstuvwxyz")

         # Send individual letters
        for letter in alphabet_list:
            send_to_queue("salary-app1-queue1", [letter])

        # # Create a list of keys
        # keys = [''.join(combination) for combination in itertools.product(alphabet_list, repeat=2)]

        # # Split keys into chunks of size 26 (for each letter of the alphabet)
        # chunk_size = 26
        # key_chunks = [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]
       
        # # Send each chunk as a separate message to salary-app1-queue1 queue
        # for chunk in key_chunks:
        #     send_to_queue("salary-app1-queue1", chunk)

    except Exception as ex:
        logging.error(f"Error: {ex}")
        return func.HttpResponse(f"Internal Server Error: {ex}", status_code=500)

    return func.HttpResponse("All keys have been sent to the salary-app1-queue1 queue.", mimetype="text/plain", status_code=200)


