import requests
import xmltodict
from sqlalchemy import create_engine, MetaData, Table, insert, select, text
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import sys
import pandas as pd
from sqlalchemy.orm import sessionmaker


# Load environment variables
load_dotenv()

juniper_pass = os.getenv("JUNIPER_PASS")
juniper_mail = os.getenv("JUNIPER_EMAIL")


# Set up logging
logging.basicConfig(
    filename="juniper_list_data_local_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection strings
DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine_L1 = create_engine(DATABASE_URL_LOCAL)
Session_L1 = sessionmaker(bind=local_engine_L1)
session_L1 = Session_L1()

DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(DATABASE_URL_LOCAL)
Session_2 = sessionmaker(bind=local_engine)
session_2 = Session_2()

metadata_local_L1 = MetaData()
metadata_local = MetaData()

metadata_local_L1.reflect(bind=local_engine_L1)
metadata_local.reflect(bind=local_engine)

metadata_local = Table("juniper", metadata_local, autoload_with=local_engine)
metadata_local_L1 = Table("innova_hotels_main", metadata_local_L1, autoload_with=local_engine_L1)



def get_hotel_id_list(engine, table):
    """
    Fetch list of unique HotelIds from the given table.
    """
    with engine.connect() as conn:
        query = select(table.c.HotelId)
        df = pd.read_sql(query, conn)
        hotel_id_list = df['HotelId'].tolist()
    logger.info(f"Fetched {len(hotel_id_list)} unique hotel IDs from the table.")
    return hotel_id_list


def read_tracking_file(file_path):
    """
    Reads tracking file and returns a list of cities yet to be processed.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file.readlines()]
    logger.warning(f"Tracking file not found at {file_path}, returning empty list.")
    return []


def write_tracking_file(file_path, cities):
    """
    Writes the remaining cities to the tracking file.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        file.write("\n".join(cities))
    logger.info(f"Updated tracking file at {file_path} with {len(cities)} cities.")


def get_payload(password, email, hotel_code):
    """
    Constructs the payload for the SOAP request.
    """
    payload = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://www.juniper.es/webservice/2007/">
    <soapenv:Header/>
    <soapenv:Body>
        <HotelContent>
            <HotelContentRQ Version="1" Language="en">
                <Login Password="{password}" Email="{email}"/>
                <HotelContentList>
                    <Hotel Code="{hotel_code}"/>
                </HotelContentList>
            </HotelContentRQ>
        </HotelContent>
    </soapenv:Body>
</soapenv:Envelope>"""
    return payload


def get_data_using_juniper_api(hotel_code):
    """
    Makes a request to the Juniper API to fetch hotel data for a given hotel code.
    """
    url = "https://xml-uat.bookingengine.es/WebService/jp/operations/staticdatatransactions.asmx"

    juniper_pass = os.getenv("JUNIPER_PASS")
    juniper_mail = os.getenv("JUNIPER_EMAIL")

    payload = get_payload(juniper_pass, juniper_mail, hotel_code)
    headers = {
        'Content-Type': 'text/xml;charset=UTF-8',
        'SOAPAction': '"http://www.juniper.es/webservice/2007/HotelContent"',
        'Cookie': 'StatC=8l+HAPXk1NeIsJR8qq/ISw==; StatP=UAvxTZg5/KSb5xjepzejGq+uRWuHJ5ucWURdLMNyct+5jbHBZLi9nj/RVgL8/LVZ; idioma=en'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()  # Will raise an HTTPError for bad responses
        data_dict = xmltodict.parse(response.text)
        logger.info(f"Successfully fetched data for hotel code {hotel_code}.")
        return data_dict
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for hotel code {hotel_code}: {e}")
        return None


def update_data_in_innova_table(hotel_code):
    """
    Updates data in the innova table with hotel content fetched from Juniper API.
    """
    try:
        hotel_data = get_data_using_juniper_api(hotel_code=hotel_code)
        if not hotel_data:
            return None
        
        hotel_content = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']
        
        # Get primary image here.
        primary_image = hotel_content.get('Images', {}).get('Image', [])[0]
        primary_file_name = primary_image.get("FileName") if primary_image else None
        
        # Get all feature.
        features = hotel_content.get('Features', {}).get('Feature', [])
        features_data = []

        for feature in features:
            feature_text = feature.get("#text", None).strip() if feature.get("#text") else None
            features_data.append(feature_text)

        for i in range(len(features_data), 5):
            features_data.append(None)

        amenities = {}
        for idx in range(1, 6):
            amenities[f"Amenities_{idx}"] = features_data[idx - 1]

        data = {
            'SupplierCode': 'Juniper',
            'HotelId': hotel_content['@Code'],
            'City': None,
            'PostCode': hotel_content.get("Address", {}).get("PostalCode", None),
            'Country': hotel_content['Address']['Address'].split(',')[-1].strip(),
            'CountryCode': hotel_content['Address']['Address'].split(',')[-1].strip(),
            'HotelName': hotel_content['HotelName'],
            'Latitude': hotel_content.get("Address", {}).get("Latitude", None),
            'Longitude': hotel_content.get("Address", {}).get("Longitude", None),
            'PrimaryPhoto': primary_file_name,
            'AddressLine1': hotel_content.get("Address", {}).get("Address", None),
            'AddressLine2': hotel_content['Address']['Address'].split(',')[-1].strip(),
            'HotelReview': None,
            'Website': None,
            'ContactNumber': None,
            'FaxNumber': None,
            'HotelStar': hotel_content.get('HotelCategory', {}).get('#text', None)
        }

        data.update(amenities)
        logger.info(f"Successfully updated data for hotel ID {hotel_code}.")
        return data

    except IndexError as e:
        logger.error(f"IndexError for Hotel ID {hotel_code}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for Hotel ID {hotel_code}: {e}")
        return None


def insert_data_to_inno_table_with_tracking(engine, table, chunk_size, tracking_file_path):
    """
    Inserts data into the innova table in chunks, with tracking.
    """
    tracking_ids = read_tracking_file(tracking_file_path)
    logger.info(f"Tracking IDs loaded: {len(tracking_ids)} remaining.")

    if not tracking_ids:
        hotel_list_id = get_hotel_id_list(engine=local_engine, table=metadata_local)
        write_tracking_file(tracking_file_path, hotel_list_id)
        tracking_ids = hotel_list_id
        logger.info(f"Tracking file created with {len(tracking_ids)} IDs.")

    chunk = []
    for hotel_id in tracking_ids.copy():
        try:
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(1) FROM {table.name} WHERE HotelId = :hotel_id")
                result = conn.execute(query, {"hotel_id": hotel_id}).scalar()

            if result > 0:
                logger.info(f"Hotel ID {hotel_id} already exists. Skipping.")
                tracking_ids.remove(hotel_id)
                continue
            
            hotel_data = update_data_in_innova_table(hotel_code=hotel_id)

            if hotel_data:
                chunk.append(hotel_data)
            
            if len(chunk) >= chunk_size:
                try:
                    with engine.connect() as conn:
                        conn.execute(table.insert(), chunk)
                        conn.commit()
                        logger.info(f"Successfully inserted a chunk of {len(chunk)} records.")
                        chunk = []
                except Exception as e:
                    logger.error(f"Error inserting chunk: {e}")
            
            tracking_ids.remove(hotel_id)
            write_tracking_file(tracking_file_path, tracking_ids)

        except Exception as e:
            logger.error(f"Error processing hotel ID {hotel_id}: {e}")
            continue

    if chunk:
        try:
            with engine.connect() as conn:
                conn.execute(table.insert(), chunk)
                conn.commit()
                logger.info(f"Successfully inserted the last chunk of {len(chunk)} records.")
        except Exception as e:
            logger.error(f"Error inserting last chunk: {e}")


# Call function to insert data
insert_data_to_inno_table_with_tracking(engine=local_engine, table=metadata_local, chunk_size=10, tracking_file_path="tracking_file.txt")
sys.exit()