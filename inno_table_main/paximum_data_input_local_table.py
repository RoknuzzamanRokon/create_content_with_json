from sqlalchemy import create_engine, Table, MetaData, insert, select, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import ast
import requests
from dotenv import load_dotenv

load_dotenv()

# Database connection setup
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine_L1 = create_engine(DATABASE_URL_LOCAL)
Session_L1 = sessionmaker(bind=local_engine_L1)
session_L1 = Session_L1()


DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(DATABASE_URL_SERVER)
Session_1 = sessionmaker(bind=server_engine)
session_1 = Session_1()


DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(DATABASE_URL_LOCAL)
Session_2 = sessionmaker(bind=local_engine)
session_2 = Session_2()

metadata_server = MetaData()
metadata_local = MetaData()

metadata_server.reflect(bind=server_engine)
metadata_local.reflect(bind=local_engine)

metadata_local = Table("Paximum", metadata_local, autoload_with=local_engine)
metadata_server = Table("innova_hotels_main", metadata_server, autoload_with=local_engine_L1)


def get_data_from_paximum_api(hotel_id):
    try:
        paximum_token = os.getenv("PAXIMUM_TOKEN")

        url = "http://service.stage.paximum.com/v2/api/productservice/getproductInfo"

        payload = json.dumps({
        "productType": 2,
        "ownerProvider": 2,
        "product": hotel_id,
        "culture": "en-US"
        })
        headers = {
        'Content-Type': 'application/json',
        'Authorization': paximum_token
        }

        response = requests.request("POST", url, headers=headers, data=payload)


        # print(type(response.text))
        try:
            response_dict = json.loads(response.text)
            hotel_data = response_dict.get("body", {}).get("hotel", {})
            # print(hotel_data)
            # print(type(response_dict))
            return hotel_data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error {e}")



def updata_data_in_innova_table(hotel_id):
    try:
        hotel_data = get_data_from_paximum_api(hotel_id)

        if not hotel_data:
            print(f"No data found for hotel ID {hotel_id}")
            return None

        hotel_address = hotel_data.get("address", {}) or {}
        
        seasons = hotel_data.get("seasons", [])
        facility_categories = seasons[0].get("facilityCategories", []) if seasons else []
        facilities = facility_categories[0].get("facilities", []) if facility_categories else []

        amenities = {
            f"Amenities_{idx}": facilities[idx - 1].get("name", None) if len(facilities) >= idx else None
            for idx in range(1, 6)
        }

        data = {
            'SupplierCode': 'Paximum',
            'HotelId': hotel_data.get("id", None),
            'City': hotel_address.get("city", {}).get("name", None),
            'PostCode': hotel_address.get("zipCode", None),
            'Country': hotel_data.get("country", {}).get("name", None),
            'CountryCode': hotel_data.get("country", {}).get("id", None),
            'HotelName': hotel_data.get("name", None),
            'Latitude': hotel_address.get("geolocation", {}).get("latitude", None),
            'Longitude': hotel_address.get("geolocation", {}).get("longitude", None),
            'PrimaryPhoto': hotel_data.get("thumbnailFull", None),
            'AddressLine1': hotel_address.get("addressLines", [None, None])[0],  
            'AddressLine2': hotel_address.get("addressLines", [None, None])[1],
            'HotelReview': hotel_data.get("rating", None),
            'Website': hotel_data.get("homePage", None),
            'ContactNumber': hotel_data.get("phoneNumber", None),
            'FaxNumber': hotel_data.get("faxNumber", None),
            'HotelStar': hotel_data.get("stars", None),
        }

        data.update(amenities)

        missing_fields = [key for key, value in data.items() if value is None]
        if missing_fields:
            print(f"Missing fields for Hotel ID {hotel_id}: {missing_fields}")

        return data
    except IndexError as e:
        print(f"IndexError for Hotel ID {hotel_id}: {e}")
        return None  
    except Exception as e:
        print(f"Unexpected error for Hotel ID {hotel_id}: {e}")
        return None



def get_hotel_id_list(engine, table):
    with engine.connect() as conn:
        query = select(table.c.HotelId)
        
        df = pd.read_sql(query, conn)
        
        hotel_id_list = df['HotelId'].tolist()
        
    return hotel_id_list



def insert_data_to_inno_table(engine, table, chunk_size):
    hotel_list_id = get_hotel_id_list(local_engine, metadata_local)

    # Check if hotel_list_id is empty
    if not hotel_list_id:
        print("No hotel IDs found in the list.")
        return

    chunk = []
    for hotel_id in hotel_list_id:
        try:
            # Check if hotel_id already exists in the table
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(1) FROM {table.name} WHERE HotelId = :hotel_id")
                result = conn.execute(query, {"hotel_id": hotel_id}).scalar()

            if result > 0:
                print(f"Hotel ID {hotel_id} already exists. Skipping.")
                continue

            # Fetch hotel data
            hotel_data = updata_data_in_innova_table(hotel_id=hotel_id)

            if hotel_data:
                chunk.append(hotel_data)

            if len(chunk) >= chunk_size:
                try:
                    with engine.connect() as conn:
                        conn.execute(table.insert(), chunk)
                        conn.commit()
                        print(f"Successfully inserted a chunk of {len(chunk)} records.")
                        chunk = []  # Reset the chunk after inserting
                except Exception as e:
                    print(f"Error inserting chunk: {e}")

        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")

    # Insert any remaining data in the last chunk (less than the chunk_size)
    if chunk:
        try:
            with engine.connect() as conn:
                conn.execute(table.insert(), chunk)
                conn.commit()
                print(f"Successfully inserted the last chunk of {len(chunk)} records.")
        except Exception as e:
            print(f"Error inserting last chunk: {e}")




# hotel_list_id = get_hotel_id_list(local_engine, metadata_local)
# print(hotel_list_id)


# hotel_data = get_data_from_paximum_api(hotel_id="231182")
# print(hotel_data)

# data = updata_data_in_innova_table(hotel_id="231182")
# print(data)


insert_data_to_inno_table(local_engine_L1, metadata_server, chunk_size=1000)
