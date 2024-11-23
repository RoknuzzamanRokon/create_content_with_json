import os
import json
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
from io import StringIO 


load_dotenv()

# Configure logging
logging.basicConfig(
    filename='paximum_data_processing.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
paximum_table = Table('paximum', metadata, autoload_with=engine)

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(DATABASE_URL_SERVER)



def authentication_paximum():
    paximum_token = os.getenv("PAXIMUM_TOKEN")
    paximum_agency = os.getenv("PAXIMUM_AGENCY")
    paximum_user = os.getenv("PAXIMUM_USER")
    paximum_password = os.getenv("PAXIMUM_PASSWORD")
    
    url = "http://service.stage.paximum.com/v2/api/authenticationservice/login"

    payload = json.dumps({
        "Agency": paximum_agency,
        "User": paximum_user,
        "Password": paximum_password
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': paximum_token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        try:
            df = pd.read_json(StringIO(response.text))
            token = df.get("body").get("token")
            return token
        except Exception as e:
            print("Error parsing token:", e)
            return None
    else:
        print(f"Failed to authenticate. Status code: {response.status_code}, Response: {response.text}")
        return None


def get_city_name(table, engine, column):
    """Fetch distinct city names from the specified table."""
    try:
        query = f"SELECT DISTINCT {column} FROM {table}"
        df = pd.read_sql(query, engine)
        city_names = df[column].dropna().tolist()
        logging.info(f"Successfully fetched city names from {table}")
        return city_names
    except Exception as e:
        logging.error(f"Error fetching city names: {e}")
        return []


def insert_data_to_paximum(data, engine, table):
    """Insert data into the paximum table and save immediately."""
    with engine.connect() as conn:
        for item in data.get("body", {}).get("items", []):
            hotel = item.get("hotel")
            if hotel:
                hotel_id = hotel.get("id")

                query = select(table.c.HotelId).where(table.c.HotelId == hotel_id)
                result = conn.execute(query).fetchone()

                if result:
                    logging.info(f"HotelId {hotel_id} already exists. Skipping.")
                    continue  

                insert_data = {
                    "HotelName": hotel.get("internationalName") or None,
                    "HotelId": hotel_id,
                    "CountryName": item.get("country", {}).get("name") or None,
                    "CountryCode": item.get("country", {}).get("id"),
                    "Longitude": item.get("geolocation", {}).get("longitude") or None,
                    "Latitude": item.get("geolocation", {}).get("latitude") or None,
                }

                # Insert into the database immediately
                try:
                    conn.execute(table.insert().values(insert_data))
                    conn.commit()  
                    logging.info(f"Inserted data for HotelId: {hotel_id}")
                except IntegrityError as e:
                    logging.warning(f"Duplicate entry for HotelId {hotel_id}. Details: {e}")
                except Exception as e:
                    logging.error(f"Error inserting data for HotelId {hotel_id}. Details: {e}")


def read_tracking_file(file_path):
    """
    Reads tracking file and returns a list of cities yet to be processed.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file.readlines()]
    return []

def write_tracking_file(file_path, cities):
    """
    Writes the remaining cities to the tracking file.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        file.write("\n".join(cities))

def initialize_tracking_file(file_path, city_list):
    """
    Initializes the tracking file if it does not exist.
    """
    if not os.path.exists(file_path):
        write_tracking_file(file_path, city_list)
        logging.info(f"Tracking file created with {len(city_list)} cities.")
    else:
        logging.info(f"Tracking file already exists with {len(read_tracking_file(file_path))} cities remaining.")







# # Fetch city names from the source database
# city_column = 'hotel_city'
# source_table = 'vervotech_mapping'
# city_names = get_city_name(table=source_table, engine=server_engine, column=city_column)


# token = authentication_paximum()


# for city in city_names:
#     payload = json.dumps({
#         "ProductType": 2,
#         "Query": city,
#         "Culture": "en-US"
#     })
#     headers = {
#         'Content-Type': 'application/json',
#         'Authorization': f'Bearer {token}'
#     }

#     try:
#         response = requests.post(url, headers=headers, data=payload)
#         if response.status_code == 200:
#             response_data = response.json()
#             insert_data_to_paximum(response_data, engine, paximum_table)
#             logging.info(f"Processed city: {city}")
#         else:
#             logging.warning(f"Failed to fetch data for city: {city}, Status Code: {response.status_code}")
#     except requests.RequestException as e:
#         logging.error(f"Error while making API call for city: {city}, Error: {e}")

# logging.info("Data processing completed.")








city_column = 'hotel_city'
source_table = 'vervotech_mapping'
tracking_file_path = "update_city_info.txt"

city_names = get_city_name(table=source_table, engine=server_engine, column=city_column)
initialize_tracking_file(tracking_file_path, city_names)

remaining_cities = read_tracking_file(tracking_file_path)

token = authentication_paximum()
url = "http://service.stage.paximum.com/v2/api/productservice/getarrivalautocomplete"

for city in remaining_cities.copy():  
    payload = json.dumps({
        "ProductType": 2,
        "Query": city,
        "Culture": "en-US"
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            response_data = response.json()
            insert_data_to_paximum(response_data, engine, paximum_table)
            logging.info(f"Successfully processed city: {city}")
            remaining_cities.remove(city)  
            write_tracking_file(tracking_file_path, remaining_cities) 
        else:
            logging.warning(f"Failed to fetch data for city: {city}, Status Code: {response.status_code}")
    except requests.RequestException as e:
        logging.error(f"Error while making API call for city: {city}, Error: {e}")

logging.info("City processing completed.")