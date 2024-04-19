"""
This is the queue trigger 2 script when messages recieves in queue2 it will be trigggerd and 
it scrapes the data and print it.
"""
# import azure.functions as func
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from bs4 import BeautifulSoup
# import re

# def main(msg: func.QueueMessage) -> None:
#     # Define your Azure Storage connection string
#     connection_string = ""

#     try:
#         # Scrape data from the URL using Selenium
#         message_content = msg.get_body().decode("utf-8")
#         scraped_data = scrape_data_with_selenium(message_content)

#         # Print the scraped data for the current URL
#         print(f"{message_content} scraped data:\n{scraped_data}\n")

#     except Exception as e:
#         # Handle errors gracefully, log them, and don't raise exceptions
#         print(f"Error processing message: {str(e)}")

# def scrape_data_with_selenium(url):
#     driver = None  # Initialize the driver variable

#     try:
#         # Initialize a Selenium WebDriver (Chrome in this example)
#         options = webdriver.ChromeOptions()
#         options.add_argument("--headless")  # Run headless (without a graphical user interface)
#         driver = webdriver.Chrome(options=options)  # Initialize Chrome WebDriver with options

#         # Navigate to the URL
#         driver.get(url)

#         # Scrape profile name
#         profile_name_element = driver.find_element(By.CSS_SELECTOR, 'h1[data-qa-target="ProviderDisplayName"]')
#         profile_name = profile_name_element.text

#         # Scrape profile specialty
#         profile_specialty_element = driver.find_element(By.CSS_SELECTOR, 'span[data-qa-target="ProviderDisplaySpeciality"]')
#         profile_specialty = profile_specialty_element.text

#         # Scrape profile gender
#         profile_gender_element = driver.find_element(By.CSS_SELECTOR, 'span[data-qa-target="ProviderDisplayGender"]')
#         profile_gender = profile_gender_element.text

#         # Extract office locations
#         office_locations_elements = driver.find_elements(By.CSS_SELECTOR, '.office-location-content-suggest-an-edit-layout .address')
#         location_columns = {}  # Create a dictionary to store location columns

#         for i, location_element in enumerate(office_locations_elements, 1):
#             # Split the text into address name and address
#             parts = location_element.text.split('\n', 1)
#             if len(parts) == 2:
#                 address_name, address = parts
#                 location_columns[f"Address {i} Name"] = address_name
#                 location_columns[f"Address {i} Location"] = address

#         # Extract biography
#         biography_element = driver.find_element(By.CSS_SELECTOR, 'span[data-qa-target="about-me-details"]')
#         biography_text = biography_element.text

#         # Extract phone numbers without "tel:" prefix
#         phone_numbers = []

#         # Parse the HTML content of the page using BeautifulSoup
#         soup = BeautifulSoup(driver.page_source, "html.parser")

#         # Find all anchor tags with "href" attributes starting with "tel:"
#         for a in soup.find_all('a', href=True):
#             href = a['href']
#             # Check if the href attribute starts with "tel:" and matches the phone number pattern
#             if href.startswith("tel:") and re.match(r'tel:\(\d{3}\) \d{3}-\d{4}', href):
#                 # Remove the "tel:" prefix
#                 phone_number = href[4:]
#                 phone_numbers.append(phone_number)

#         # Initialize the scraped_data dictionary
#         scraped_data = {
#             "Profile Name": profile_name,
#             "Specialty": profile_specialty,
#             "Gender": profile_gender,
#             "Biography": biography_text,
#         }

#         # Append additional data to the scraped_data dictionary
#         for i in range(1, 4):  # Assuming a maximum of 3 addresses and phone numbers
#             scraped_data[f"Address {i} Name"] = location_columns.get(f"Address {i} Name", '')
#             scraped_data[f"Address {i} Location"] = location_columns.get(f"Address {i} Location", '')
#             scraped_data[f"Address {i} Phone Number"] = phone_numbers[i - 1] if i <= len(phone_numbers) else ''

#         # Now, the scraped_data dictionary contains all the scraped data

#         return scraped_data

#     except Exception as e:
#         return f"Error while scraping data with Selenium for URL: {url}\n{str(e)}"

#     finally:
#         if driver:
#             # Close the WebDriver to free up resources
#             driver.quit()

# ===============================================
"""
This code will use async and await technique and scrape data from queue2 and print it.
"""

# import azure.functions as func
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from bs4 import BeautifulSoup
# import re
# import asyncio

# async def main(msg: func.QueueMessage) -> None:
#     # Define your Azure Storage connection string
#     connection_string = ""

#     try:
#         # Scrape data from the URL using Selenium
#         message_content = msg.get_body().decode("utf-8")
#         scraped_data = await scrape_data_with_selenium(message_content)

#         # Print the scraped data for the current URL
#         print(f"{message_content} scraped data:\n{scraped_data}\n")

#     except Exception as e:
#         # Handle errors gracefully, log them, and don't raise exceptions
#         print(f"Error processing message: {str(e)}")

# async def scrape_data_with_selenium(url):
#     driver = None  # Initialize the driver variable

#     try:
#         # Initialize a Selenium WebDriver (Chrome in this example)
#         options = webdriver.ChromeOptions()
#         options.add_argument("--headless")  # Run headless (without a graphical user interface)
#         driver = webdriver.Chrome(options=options)  # Initialize Chrome WebDriver with options

#         # Navigate to the URL
#         await asyncio.to_thread(driver.get, url)  # Use asyncio to call driver.get asynchronously

#         # Scrape profile name
#         profile_name_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'h1[data-qa-target="ProviderDisplayName"]')
#         profile_name = profile_name_element.text

#         # Scrape profile specialty
#         profile_specialty_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'span[data-qa-target="ProviderDisplaySpeciality"]')
#         profile_specialty = profile_specialty_element.text

#         # Scrape profile gender
#         profile_gender_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'span[data-qa-target="ProviderDisplayGender"]')
#         profile_gender = profile_gender_element.text

#         # Extract office locations
#         office_locations_elements = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, '.office-location-content-suggest-an-edit-layout .address')
#         location_columns = {}  # Create a dictionary to store location columns

#         for i, location_element in enumerate(office_locations_elements, 1):
#             # Split the text into address name and address
#             parts = location_element.text.split('\n', 1)
#             if len(parts) == 2:
#                 address_name, address = parts
#                 location_columns[f"Address {i} Name"] = address_name
#                 location_columns[f"Address {i} Location"] = address

#         # Extract biography
#         biography_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'span[data-qa-target="about-me-details"]')
#         biography_text = biography_element.text

#         # Extract phone numbers without "tel:" prefix
#         phone_numbers = []

#         # Parse the HTML content of the page using BeautifulSoup
#         soup = BeautifulSoup(driver.page_source, "html.parser")

#         # Find all anchor tags with "href" attributes starting with "tel:"
#         for a in soup.find_all('a', href=True):
#             href = a['href']
#             # Check if the href attribute starts with "tel:" and matches the phone number pattern
#             if href.startswith("tel:") and re.match(r'tel:\(\d{3}\) \d{3}-\d{4}', href):
#                 # Remove the "tel:" prefix
#                 phone_number = href[4:]
#                 phone_numbers.append(phone_number)

#         # Initialize the scraped_data dictionary
#         scraped_data = {
#             "Profile Name": profile_name,
#             "Specialty": profile_specialty,
#             "Gender": profile_gender,
#             "Biography": biography_text,
#         }

#         # Append additional data to the scraped_data dictionary
#         for i in range(1, 4):  # Assuming a maximum of 3 addresses and phone numbers
#             scraped_data[f"Address {i} Name"] = location_columns.get(f"Address {i} Name", '')
#             scraped_data[f"Address {i} Location"] = location_columns.get(f"Address {i} Location", '')
#             scraped_data[f"Address {i} Phone Number"] = phone_numbers[i - 1] if i <= len(phone_numbers) else ''

#         # Now, the scraped_data dictionary contains all the scraped data

#         return scraped_data

#     except Exception as e:
#         return f"Error while scraping data with Selenium for URL: {url}\n{str(e)}"

#     finally:
#         if driver:
#             # Close the WebDriver to free up resources
#             await asyncio.to_thread(driver.quit)

# =======================================================================================
"""
This code will scrape data and send the scrape data to database table .
"""
import azure.functions as func
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
import pyodbc
import asyncio
import json
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging

async def main(msg: func.QueueMessage) -> None:
    server = ''
    database = ''
    username = ''
    password = ''
    table_name = ''

    try:
        # Scrape data from the URL using Selenium
        message_content = msg.get_body().decode("utf-8")
        scraped_data = await scrape_data_with_selenium(message_content)
        send_data_to_database(scraped_data, server, database, username, password, table_name)

    except Exception as e:
        print(f"Error processing message: {str(e)}")

async def scrape_data_with_selenium(url):
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
        
        await asyncio.to_thread(driver.get, url)  # Use asyncio to call driver.get asynchronously

        # Scrape profile name
        profile_name_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'h1[data-qa-target="ProviderDisplayName"]')
        profile_name = profile_name_element.text

        # Scrape profile specialty
        profile_specialty_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'span[data-qa-target="ProviderDisplaySpeciality"]')
        profile_specialty = profile_specialty_element.text

        # Scrape profile gender
        profile_gender_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'span[data-qa-target="ProviderDisplayGender"]')
        profile_gender = profile_gender_element.text

        # Extract office locations
        office_locations_elements = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, '.office-location-content-suggest-an-edit-layout .address')
        location_columns = {}  # Create a dictionary to store location columns

        for i, location_element in enumerate(office_locations_elements, 1):
            # Split the text into address name and address
            parts = location_element.text.split('\n', 1)
            if len(parts) == 2:
                address_name, address = parts
                location_columns[f"Address{i}Name"] = address_name
                location_columns[f"Address{i}Location"] = address

        # Extract biography
        biography_element = await asyncio.to_thread(driver.find_element, By.CSS_SELECTOR, 'span[data-qa-target="about-me-details"]')
        biography_text = biography_element.text

        # Extract phone numbers without "tel:" prefix
        phone_numbers = []

        
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Find all anchor tags with "href" attributes starting with "tel:"
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Check if the href attribute starts with "tel:" and matches the phone number pattern
            if href.startswith("tel:") and re.match(r'tel:\(\d{3}\) \d{3}-\d{4}', href):
                # Remove the "tel:" prefix
                phone_number = href[4:]
                phone_numbers.append(phone_number)

        # Initialize the scraped_data dictionary
        scraped_data = {
            "ProfileName": profile_name,
            "Specialty": profile_specialty,
            "Gender": profile_gender,
            "Biography": biography_text,
        }

        # Append additional data to the scraped_data dictionary
        for i in range(1, 6):  
            scraped_data[f"Address{i}Name"] = location_columns.get(f"Address{i}Name", '')
            scraped_data[f"Address{i}Location"] = location_columns.get(f"Address{i}Location", '')
            scraped_data[f"Address{i}Number"] = phone_numbers[i - 1] if i <= len(phone_numbers) else ''


        return scraped_data

    except Exception as e:
        return f"Error while scraping data with Selenium for URL: {url}\n{str(e)}"

    finally:
        if driver:
            await asyncio.to_thread(driver.quit)

def send_data_to_database(data, server, database, username, password, table_name):
    try:
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Define an INSERT SQL statement with parameter placeholders
        insert_sql = f"INSERT INTO {table_name} (ProfileName, Specialty, Gender, Biography, Address1Name, Address1Location, Address1Number, Address2Name, Address2Location, Address2Number, Address3Name, Address3Location, Address3Number, Address4Name, Address4Location, Address4Number, Address5Name, Address5Location, Address5Number) " \
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        # Insert the scraped data into the table
        cursor.execute(
            insert_sql,
            (
                data["ProfileName"],
                data["Specialty"],
                data["Gender"],
                data["Biography"],
                data["Address1Name"],
                data["Address1Location"],
                data["Address1Number"],
                data["Address2Name"],
                data["Address2Location"],
                data["Address2Number"],
                data["Address3Name"],
                data["Address3Location"],
                data["Address3Number"],
                data["Address4Name"],
                data["Address4Location"],
                data["Address4Number"],
                data["Address5Name"],
                data["Address5Location"],
                data["Address5Number"],
            ),
        )

        # Commit the changes and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()

        print("Data uploaded to the table successfully.")

    except Exception as e:
        print(f"Error sending data to the database: {str(e)}")
