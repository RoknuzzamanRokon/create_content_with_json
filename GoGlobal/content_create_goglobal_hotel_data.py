from sqlalchemy import create_engine, Table, MetaData, insert, select, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import ast
import requests
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO 
import random
import xml.etree.ElementTree as ET
import xmltodict


load_dotenv()

# Database connection setup
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL_SERVER)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
vervotech_table = Table("vervotech_mapping", metadata, autoload_with=engine)

def get_data_from_vervotech_mapping_table(engine):
    query = "SELECT ProviderHotelId, ProviderFamily, hotel_city, hotel_country, hotel_longitude, hotel_latitude FROM vervotech_mapping WHERE ProviderFamily = 'GoGlobal'"
    df = pd.read_sql(query, engine)
    result_list = df.to_records(index=False).tolist()
    return result_list

def save_data_to_file(data, file_name):
    data_as_string = "\n".join([str(item) for item in data])
    file_path = os.path.join(os.getcwd(), file_name)
    
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(data_as_string)
    
    print(f"Data successfully saved to {file_path}")


def get_data_from_goglobal_api():
    url = "https://gtr.xml.goglobal.travel/xmlwebservice.asmx"

    payload = """<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                    xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
            <soap12:Body>
                <MakeRequest xmlns="http://www.goglobal.travel/">
                        <requestType>6</requestType>
                        <xmlRequest>
                            <![CDATA[
                                <Root>
                                    <Header>
                                        <Agency>149548</Agency>
                                        <User>NOFSHONXMLTEST</User>
                                        <Password>W99IL98KY1G</Password>
                                        <Operation>HOTEL_INFO_REQUEST</Operation>
                                        <OperationType>Request</OperationType>
                                    </Header>
                                    <Main Version="2.2">
                                        <InfoHotelId>10000</InfoHotelId>
                                        <InfoLanguage>en</InfoLanguage>
                                    </Main>
                                </Root>
                            ]]>
                        </xmlRequest>
                </MakeRequest>
            </soap12:Body>
        </soap12:Envelope>"""


    headers = {
            'Content-Type': 'application/soap+xml; charset=utf-8',
            'API-Operation': 'HOTEL_INFO_REQUEST'
            }


    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status() 
        parsed_response = xmltodict.parse(response.text)
        xml_data = parsed_response["soap:Envelope"]["soap:Body"]["MakeRequestResponse"]["MakeRequestResult"]
        root_data = ET.fromstring(xml_data)
        return root_data       
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An error occurred during parsing: {e}")


goglobal_data = get_data_from_goglobal_api()



# data = get_data_from_vervotech_mapping_table(engine=engine)
# print(data)

# data = get_data_from_vervotech_mapping_table(engine=engine)
# file_name = "collect_some_goglobal_data.txt"
# save_data_to_file(data, file_name)