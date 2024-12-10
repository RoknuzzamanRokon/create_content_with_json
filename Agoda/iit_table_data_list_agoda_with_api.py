from sqlalchemy import create_engine, select, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json 
import os
import requests
from dotenv import load_dotenv
import xmltodict


load_dotenv()

DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=local_engine)
session = Session()
metadata = MetaData()

table_name = Table("innova_hotels_main", metadata, autoload_with=local_engine)


gtrs_api_key = os.getenv("AGODA_API_KEY")


def update_data_for_innova_table(hotel_id):
    gtrs_api_key = os.getenv("AGODA_API_KEY")
    url = f"https://affiliatefeed.agoda.com/datafeeds/feed/getfeed?apikey={gtrs_api_key}&mhotel_id={hotel_id}&feed_id=19"
    response = requests.get(url=url)

    if response.status_code == 200:
        xml_data = response.content
        data_dict = xmltodict.prase(xml_data)

        hotel_feed_full = data_dict.get("Hotel_feed_full")
        if hotel_feed_full is None:
            print(f"Skipping hotel {hotel_id} as 'Hotel feed full' is not found")
            return None
        
        hotel_data = hotel_feed_full.get("hotels", {}).get("hotel", {})

        if not hotel_data.get("hotel_id"):
            print(f"Skipping hotel {hotel_id} as 'hotel_id' is not found.")
            return None

        # Facilities processing for amenities
        facilities_types = hotel_feed_full.get("facilities")
        facilities_list = []
        if facilities_types:
            facilities = facilities_types.get("facility", [])
            if isinstance(facilities, list):
                for facility in facilities:
                    if isinstance(facility, dict):
                        facilities_list.append(facility.get("property_group_description", None))
        
        # Build amenities dictionary safely
        amenities = {}
        for idx in range(1, 6):
            try:
                amenities[f"Amenities_{idx}"] = facilities_list[idx - 1]
            except IndexError:
                amenities[f"Amenities_{idx}"] = None


        data = {
            'SupplierCode': 'Agoda',
            'HotelId': hotel_data.get("hotel_id", None),
            'City': hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("city", None),
            'PostCode': hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("postal_code", None),
            'Country': hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("country", None),
            'CountryCode': None,
            'HotelName': hotel_data.get("hotel_name", None),
            'Latitude': hotel_data.get("latitude", None),
            'Longitude': hotel_data.get("longitude", None),
            'PrimaryPhoto': None,
            'AddressLine1': hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_1", None), 
            'AddressLine2': hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_2", None),
            'HotelReview': hotel_data.get("number_of_reviews", None),
            'Website': None,
            'ContactNumber': None,
            'FaxNumber': None,
            'HotelStar': hotel_data.get("star_rating", None),
        }

        data.update(amenities)
        return data
    else:
        print(f"Failed to fetch data for hotel {hotel_id}, status code: {response.status_code}")
        return None




# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL_2 = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(DATABASE_URL_2)
Session_2 = sessionmaker(bind=server_engine)
session_2 = Session_2()
metadata_2 = MetaData()

table_name_2 = Table("vervotech_mapping", metadata_2, autoload_with=server_engine)


def get_hotel_id_list(engine, table):
    with engine.connect() as conn:
        query = select(table.c.ProviderHotelId).where(table.c.ProviderFamily == 'agoda')
        
        df = pd.read_sql(query, conn)
        
        hotel_id_list = df['ProviderHotelId'].tolist()
        
    return hotel_id_list

def read_tracking_ids(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            tracking_ids = file.read().splitlines()
            return [id.strip() for id in tracking_ids if id.strip()]
    else:
        return[]
    
def write_tracking_ids(file_path, tracking_ids):
    with open(file_path, tracking_ids):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(tracking_ids))



def insert_data_to_inno_table_with_tracking(engine, table, chunk_size, tracking_file_path):
    tracking_ids = read_tracking_ids(tracking_file_path)
    print(f"Tracking IDs loaded: {len(tracking_ids)}")

    if not tracking_ids:
        hotel_list_id = get_hotel_id_list(engine=server_engine, table=table_name_2)
        write_tracking_ids(tracking_file_path, hotel_list_id)
        tracking_ids = hotel_list_id
        print(f"Tracking file created with {len(tracking_ids)} IDs.")
    
    chunk = []
    for hotel_id in tracking_ids.copy():
        try:
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(1) FROM {table.name} WHERE HotelId = :hotel_id")
                result = conn.execute(query, {"hotel_id": hotel_id}).scalar()

            if result > 0:
                print(f"Hotel ID {hotel_id} already exists. Skipping.")
                tracking_ids.remove(hotel_id)
                continue
            
            hotel_data = update_data_for_innova_table(hotel_id=hotel_id)

            if hotel_data:
                chunk.append(hotel_data)
            
            if len(chunk) >= chunk_size:
                try:
                    with engine.connect() as conn:
                        conn.execute(table.insert(), chunk)
                        conn.commit()
                        print(f"Successfully inserted a chunk of {len(chunk)} records.")
                        chunk = []
                except Exception as e:
                    print(f"Error inserting chunk: {e}")

            tracking_ids.remove(hotel_id)

            write_tracking_ids(tracking_file_path, tracking_ids)

        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")


    if chunk:
        try:
            with engine.connect() as conn:
                conn.execute(table.insert(), chunk)
                conn.commit()
                print(f"Successfully inserted the last chunk of {len(chunk)} records.")
        except Exception as e:
            print(f"Error inserting last chunk: {e}")

    write_tracking_ids(tracking_file_path, tracking_ids)
    print("Processing complete. Updated tracking file.")


tracking_file_path = "agoda_tracking_id_list.txt"

insert_data_to_inno_table_with_tracking(local_engine, metadata, chunk_size=100, tracking_file_path=tracking_file_path)
