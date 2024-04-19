

import azure.functions as func
import logging
import json
from bs4 import BeautifulSoup
import os
import pyodbc
import requests
from dateutil import parser

def clean_string(input_str):
    # Replace non-breaking hyphen with a regular hyphen
    return input_str.replace('\u2011', '-')

def main(msg: func.QueueMessage) -> None:
    server = os.environ["DbServer"]
    database = os.environ["DbName"]
    username = os.environ["DbUsername"]
    password = os.environ["DbPassword"]
    table_name = os.environ["DbTableName"]
    
    try:
        message_content = msg.get_body().decode("utf-8")
        message_list = json.loads(message_content)

        if message_list:
            for url in message_list:
                logging.info(f"url is: {url}")
                response = requests.get(url)
                # response = requests.get(quote(url, safe=":/"))
                soup = BeautifulSoup(response.text, 'html.parser')
                
               # Protester
                try:
                    try:
                        protestor_div = soup.find('div', class_='field field--name-field-protestor field--type-string field--label-inline')
                        protestor_text = protestor_div.find('div', class_='field__item').get_text(strip=True)
                    except:
                        protestor_text = soup.find('div', class_='block__inner').get_text(strip=True)
                except Exception as e:
                    protestor_text = "None" 


                # Solicitation Number
                try:
                    solicitation_div = soup.find('div', class_='field--name-field-solicitation-number')
                    solicitation_number = solicitation_div.find('div', class_='field__item').get_text(strip=True)
                except:
                    solicitation_number = "None"

                # Outcome
                try:
                    outcome_div = soup.find('div', class_='field--name-field-outcome')
                    outcome_item_div = outcome_div.find('div', class_='field__item')
                    outcome_text = outcome_item_div.get_text(strip=True)
                except:
                    outcome_text = "None"
                    

                # Decision
                decision_text = "None"
                try:
                    decision_element = soup.find('div', {'class': 'field--name-field-decision-summary'})
                    decision_text = decision_element.find('p').get_text(strip=True, separator=' ')
                    decision_text = clean_string(decision_text)
                    # logging.info(f"Descision: {decision_text}")
                except:
                    decision_div = soup.find('div', class_='field-items-wrapper')
                    if decision_div:
                        paragraphs = decision_div.find_all('p')
                        decision_index = next((index for index, paragraph in enumerate(paragraphs) if "DECISION" in paragraph.get_text()), None)

                        if decision_index is not None:
                            decision_text_1 = paragraphs[decision_index].get_text(strip=True)
                            decision_text = paragraphs[decision_index + 1].get_text(strip=True) if decision_index + 1 < len(paragraphs) else None
                            decision_text = clean_string(decision_text)
                            # logging.info(f"Descision: {decision_text}")
                        else:
                            print("Paragraph with 'DECISION' not found.")
                    else:
                        print("Decision div not found.")


                # Filed Date
                try:
                    filed_date_div = soup.find('div', class_='field--name-field-filed-date')
                    filed_date_text = filed_date_div.find('div', class_='field__item').find('time').get_text(strip=True)
                except:
                    filed_date_text = None


                # Due Date
                try:
                    due_date_div = soup.find('div', class_='field--name-field-due-date')
                    due_date_text = due_date_div.find('div', class_='field__item').find('time').get_text(strip=True)
                except:
                    due_date_text = None

                # Case Type
                try:
                    case_type_div = soup.find('div', class_='field--name-field-case-type')
                    case_type_text = case_type_div.find('div', class_='field__item').get_text(strip=True)
                except:
                    case_type_text = "None"

            


                # Agency, File number

                def extract_field_value(header_text):
                    header_element = soup.find('header', string=header_text)
                    
                    if header_element:
                        field_item = header_element.find_next('div', {'class': 'field__item'})
                        return field_item.get_text(strip=True) if field_item else None
                    else:
                        return None

                # Extracting File Number using the first method
                file_number = extract_field_value('File number')

                # If File Number is not found, try another method
                if file_number is None:
                    file_number_span = soup.find('span', class_='d-block text-small').strong
                    file_number = file_number_span.text if file_number_span else None


                # Extracting Agency Information using the first method
                agency_text = extract_field_value('Agency')
                


                # GAO Attorney
                try:
                    gao_attorney_div = soup.find('div', class_='field--name-field-gao-attorney')
                    gao_attorney_text = gao_attorney_div.find('div', class_='field__item').get_text(strip=True)
                except:
                    gao_attorney_text = "None"

                # Recommendation
                recommendation_text = "None"
                try:
                    recommendations_heading = soup.find('h2', string='Recommendations')
                    recommendation_text = recommendations_heading.find_next('p').get_text(strip=True)
                except:
                    recommendation_div = soup.find('div', class_='field-items-wrapper')
                    if recommendation_div:
                        paragraphs = recommendation_div.find_all('p')
                        recommendation_index = next((index for index, paragraph in enumerate(paragraphs) if "RECOMMENDATION" in paragraph.get_text()), None)
                        if recommendation_index is not None:
                            recommendation_text_1 = paragraphs[recommendation_index].get_text(strip=True)
                            recommendation_text = paragraphs[recommendation_index + 1].get_text(strip=True) if recommendation_index + 1 < len(paragraphs) else None
                        else:
                            print("Paragraph with 'Rcommendation' not found.")
                    else:
                        print("Recommendation div not found.")
                    
                    

                # Recommendations for Executive Action
                recommendations = []
                for recommendation_row in soup.select('tbody tr'):
                    try:
                        agency_elem = recommendation_row.select_one('.views-field-name')
                        recommendation_elem = recommendation_row.select_one('.views-field-field-recommendation')
                        status_elem = recommendation_row.select_one('.views-field-field-status-code .code')
                        comment_elem = recommendation_row.select_one('.comment')

                        if agency_elem and recommendation_elem and status_elem and comment_elem:
                            agency = agency_elem.get_text(strip=True)
                            recommendation_text = recommendation_elem.get_text(strip=True)
                            status = status_elem.get_text(strip=True)
                            comment = comment_elem.get_text(strip=True)

                            # Create a dictionary for each entry
                            entry = {
                                "Agency": agency,
                                "Recommendation": recommendation_text,
                                "Status": status,
                                "Comment": comment
                            }

                            recommendations.append(entry)

                    except AttributeError as e:
                        print(f"Error: {e}")
                        continue

                # Check if recommendations list is empty
                if not recommendations:
                    recommendations = None

                json_data = json.dumps(recommendations, indent=2)
                json_data = json_data.replace('\n', '')


                # # GAO Contacts
                contacts_list = []
                contacts = soup.find_all('article', class_='node--type-staff-profile')
                for i, contact in enumerate(contacts, start=1):
                    try:
                        name = contact.find('a', class_='node-title').text
                    except:
                        name = contact.find('span', class_='node-title').text

                    title_element = contact.find('div', class_='field--name-field-staff-contact-title')
                    title = title_element.text if title_element else None

                    email_element = contact.find('div', class_='field--name-field-staff-director-email').a
                    email = email_element['href'].replace('mailto:', '') if email_element else None

                    phone_element = contact.find('div', class_='field--name-field-staff-phone')
                    phone = phone_element.text if phone_element else None
                    entry = {
                        'Name': name,
                        'Title': title,
                        'Email': email,
                        'Phone': phone
                    }

                    contacts_list.append(entry)
                if not contacts_list:
                    contacts_list = None

                contact_json_data = json.dumps(contacts_list, indent=2)
                contact_json_data = contact_json_data.replace('\n', '')



                scraped_data = {
                            "Protestor": protestor_text,
                            "Solicitation_Number":solicitation_number,
                            "Outcome" : outcome_text,
                            "Filed_Date" : filed_date_text,
                            "Due_Date": due_date_text,
                            "Case_Type": case_type_text,
                            "Agency": agency_text,
                            "File_Number": file_number,
                            "GAO_Attorney": gao_attorney_text,
                            "Recommendation": recommendation_text,
                            "Executive_Action_Recommendation": json_data,
                            "GAO_Contacts": contact_json_data,
                            "Decision": decision_text,
                            "Published_Date": None,
                            "Publicly_Released_Date": None,
                            "Decision_Date": None
                        }


                # Decision Date, Published Date, Publicly Released Date
                try:
                    decision_date_div = soup.find('div', class_='field--name-field-decision-date')
                    scraped_data["Decision_Date"] = decision_date_div.find('div', class_='field__item').find('time').get_text(strip=True)
                except:
                    try:
                        span_texts = soup.find_all('span', class_='d-block text-small')
                        # Assuming the second span contains the decision date, modify this index if needed
                        decision_date_text = span_texts[1].text.strip()

                        if "Published" in decision_date_text and "Publicly Released" in decision_date_text:
                            # Extract the publication and release dates
                            scraped_data["Published_Date"] = decision_date_text.split('Published:')[1].split('Publicly Released:')[0].strip()
                            scraped_data["Publicly_Released_Date"] = decision_date_text.split('Publicly Released:')[1].strip()
                        else:
                            # If no "Published" and "Publicly Released" found, assume it's a decision date
                            scraped_data["Decision_Date"] = decision_date_text.strip()

                        # Convert the dates to the desired format
                        for key in ["Published_Date", "Publicly_Released_Date", "Decision_Date"]:
                            if scraped_data.get(key):
                                parsed_date = parser.parse(scraped_data[key])
                                scraped_data[key] = parsed_date.strftime("%Y-%m-%d")

                    except:
                        pass  # Handle exceptions as needed

                logging.info(f"Scraped Data:{scraped_data}")
                # print(scraped_data)
                send_data_to_database(scraped_data, server, database, username, password, table_name)
        
        else:
            logging.warning("No data found in the message list.")

    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")
   

def send_data_to_database(data, server, database, username, password, table_name):
    try:
        # Connect to the database (similar to your existing code)
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        if data is not None:
            # Check if the data already exists in the database
            select_sql = f"SELECT COUNT(*) FROM {table_name} WHERE Protestor = ? AND Solicitation_Number = ? AND Outcome = ?"
            
            cursor.execute(
                select_sql,
                (
                    data["Protestor"],
                    data["Solicitation_Number"],
                    data["Outcome"]
                ),
            )

            count = cursor.fetchone()[0]

            if count == 0:
                # Insert the scraped data into the database
                insert_sql = f"INSERT INTO {table_name} (Protestor, Solicitation_Number, Outcome, Decision_Date, Published_Date, Publicly_Released_Date, Filed_Date, Due_Date, Case_Type, Agency, File_Number, Decision, GAO_Attorney, Recommendation, Executive_Action_Recommendation, GAO_Contacts) " \
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
               
                cursor.execute(
                    insert_sql,
                    (
                        data["Protestor"],
                        data["Solicitation_Number"],
                        data["Outcome"],
                        data["Decision_Date"],
                        data["Published_Date"],
                        data["Publicly_Released_Date"],
                        data["Filed_Date"],
                        data["Due_Date"],
                        data["Case_Type"],
                        data["Agency"],
                        data["File_Number"],
                        data["Decision"],
                        data["GAO_Attorney"],
                        data["Recommendation"],
                        data["Executive_Action_Recommendation"],
                        data["GAO_Contacts"]
                        
                    ),
                )

                conn.commit()
                cursor.close()
                conn.close()
                logging.info(f"Data uploaded to the table successfully.")
            else:
                logging.info("Data already exists in the database. Skipping insertion.")
        else:
            logging.error(f"Scraping data for URL resulted in None. Data not inserted into the database.")

    except Exception as e:
        error_message = f"Error sending data to the database for URL: {str(e)}"
        logging.error(error_message)






