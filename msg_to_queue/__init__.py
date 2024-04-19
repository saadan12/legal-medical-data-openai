
"""
this code  get url from input and append it to queue 
and then queue triggered will be called and message will be printed.
this code will append only one link to queue.
"""
import azure.functions as func
import json
import base64
from azure.storage.queue import QueueServiceClient

def send_to_queue(queue_name, message, res):
    connection_string = ""  # Replace with your Azure Storage connection string

    # Create a QueueServiceClient using the connection string
    queue_service_client = QueueServiceClient.from_connection_string(connection_string)

    # Get the queue client
    queue_client = queue_service_client.get_queue_client(queue_name)

    try:
        message_base64 = base64.b64encode(message.encode('utf-8')).decode('utf-8')
        queue_client.send_message(message_base64)
        response_message = f"Message has been sent to the queue '{queue_name}'."
        res.set(response_message)
    except Exception as e:
        error_message = f"Error sending message to the queue '{queue_name}': {str(e)}"
        res.set(error_message)

def main(req: func.HttpRequest, res: func.Out[str]) -> None:
    try:
        message = req.params.get('url')

        if not message:
            res.set(func.HttpResponse("Please provide a 'message' parameter.", status_code=400))
            return
        
        queue_name = "queue1"  
        send_to_queue(queue_name, message, res)

        if res.get():
            result = {
                "message": f"Message sent to the queue '{queue_name}' successfully."
            }
            res.set(json.dumps(result))
    except Exception as ex:
        error_message = f"An error occurred: {str(ex)}"
        res.set(func.HttpResponse(error_message, status_code=500))
# ==============================================================
"""
this code get input url and then update the page number and append 108 link in the queue.
"""
# import azure.functions as func
# import json
# import base64
# from azure.storage.queue import QueueServiceClient

# def get_page_links(base_url, total_pages):
#     # Split the URL into two parts: before and after the pageNum parameter
#     url_parts = base_url.split('&pageNum=')
    
#     if len(url_parts) != 2:
#         return []

#     base_url_without_page_num = url_parts[0]
#     page_num_parameter = url_parts[1]

#     page_urls = []

#     for page_num in range(1, total_pages + 1):
#         # Update the pageNum parameter and reconstruct the URL
#         page_url = f"{base_url_without_page_num}&pageNum={page_num}&{page_num_parameter}"
#         page_urls.append(page_url)

#     return page_urls

# def send_to_queue(queue_name, messages, res):
#     connection_string = ""  # Replace with your Azure Storage connection string


#     queue_service_client = QueueServiceClient.from_connection_string(connection_string)
#     queue_client = queue_service_client.get_queue_client(queue_name)

#     try:
#         for idx, message in enumerate(messages, start=1):
#             message_base64 = base64.b64encode(message.encode('utf-8')).decode('utf-8')
            
#             # Send the base64-encoded message to the queue
#             queue_client.send_message(message_base64)

#             response_message = f"Link {idx} has been sent to the queue '{queue_name}'."
#             res.set(response_message)
#     except Exception as e:
#         error_message = f"Error sending message to the queue '{queue_name}': {str(e)}"
#         res.set(error_message)

# def main(req: func.HttpRequest, res: func.Out[str]) -> None:
#     try:

#         url = req.params.get('url')

#         if not url:
#             res.set(func.HttpResponse("Please provide a 'url' parameter.", status_code=400))
#             return

#         total_pages = 108 
#         page_urls = get_page_links(url, total_pages)
#         queue_name = "queue1"  
#         # Send all links to the same queue
#         send_to_queue(queue_name, page_urls, res)

#         if res.get():
#             result = {
#                 "message": "Page URLs sent to the queue successfully."
#             }
#             res.set(json.dumps(result))
#     except Exception as ex:
#         # Handle any unhandled exceptions at the top level
#         error_message = f"An error occurred: {str(ex)}"
#         res.set(func.HttpResponse(error_message, status_code=500))
