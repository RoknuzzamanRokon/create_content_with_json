import os
import json
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

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

paximum_token = os.getenv("PAXIMUM_TOKEN")
url = "http://service.stage.paximum.com/v2/api/productservice/getarrivalautocomplete"

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



# Fetch city names from the source database
city_column = 'hotel_city'
source_table = 'vervotech_mapping'
city_names = get_city_name(table=source_table, engine=server_engine, column=city_column)

for city in city_names:
    payload = json.dumps({
        "ProductType": 2,
        "Query": city,
        "Culture": "en-US"
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': paximum_token
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            response_data = response.json()
            insert_data_to_paximum(response_data, engine, paximum_table)
            logging.info(f"Processed city: {city}")
        else:
            logging.warning(f"Failed to fetch data for city: {city}, Status Code: {response.status_code}")
    except requests.RequestException as e:
        logging.error(f"Error while making API call for city: {city}, Error: {e}")

logging.info("Data processing completed.")
