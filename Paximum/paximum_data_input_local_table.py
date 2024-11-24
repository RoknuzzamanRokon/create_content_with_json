from sqlalchemy import create_engine, Table, MetaData, insert, select, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import ast
import requests
from dotenv import load_dotenv
from io import StringIO 

load_dotenv()


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

metadata_local = Table("Paximum", metadata_local, autoload_with=local_engine)
metadata_local_L1 = Table("innova_hotels_main", metadata_local_L1, autoload_with=local_engine_L1)


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






def get_data_from_paximum_api(hotel_id):
    try:
        token = authentication_paximum()

        url = "http://service.stage.paximum.com/v2/api/productservice/getproductInfo"

        payload = json.dumps({
            "productType": 2,
            "ownerProvider": 2,
            "product": hotel_id,
            "culture": "en-US"
            })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
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



def update_data_in_innova_table(hotel_id):
    try:
        hotel_data = get_data_from_paximum_api(hotel_id)

        if not hotel_data:
            print(f"No data found for hotel ID {hotel_id}")
            return None

        hotel_address = hotel_data.get("address", {}) or {}

        address_lines = hotel_address.get("addressLines", [])
        address_line_1 = address_lines[0] if len(address_lines) > 0 else None
        address_line_2 = address_lines[1] if len(address_lines) > 1 else None
        

        seasons = hotel_data.get("seasons", [])
        if not seasons:
            print(f"No seasons data found for Hotel ID {hotel_id}")
            facility_categories = []
            facilities = []
        else:
            facility_categories = seasons[0].get("facilityCategories", []) if len(seasons) > 0 else []
            facilities = facility_categories[0].get("facilities", []) if len(facility_categories) > 0 else []

        # Build amenities safely
        amenities = {}
        for idx in range(1, 6):
            try:
                amenities[f"Amenities_{idx}"] = facilities[idx - 1].get("name", None)
            except IndexError:
                amenities[f"Amenities_{idx}"] = None 

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
            'AddressLine1': address_line_1, 
            'AddressLine2': address_line_2,
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


def read_tracking_ids(file_path):
    """
    Reads the tracking IDs from the specified file.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            tracking_ids = file.read().splitlines()
            return [id.strip() for id in tracking_ids if id.strip()]
    else:
        return []


def write_tracking_ids(file_path, tracking_ids):
    """
    Writes the tracking IDs back to the specified file.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        file.write("\n".join(tracking_ids))

def insert_data_to_inno_table_with_tracking(engine, table, chunk_size, tracking_file_path):
    """
    Inserts data into the innova table using the tracking ID file to manage progress.
    """
    tracking_ids = read_tracking_ids(tracking_file_path)
    print(f"Tracking IDs loaded: {len(tracking_ids)} remaining")

    if not tracking_ids:
        hotel_list_id = get_hotel_id_list(engine=local_engine, table=metadata_local)
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

            # Fetch hotel data
            hotel_data = update_data_in_innova_table(hotel_id=hotel_id)

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

            # Update the tracking file
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

tracking_file_path = "tracking_id_list.txt"

# Run the insert operation
insert_data_to_inno_table_with_tracking(local_engine_L1, metadata_local_L1, chunk_size=1000, tracking_file_path=tracking_file_path)






# hotel_list_id = get_hotel_id_list(local_engine, metadata_local)
# print(hotel_list_id)


# hotel_data = get_data_from_paximum_api(hotel_id="231182")
# print(hotel_data)

# data = update_data_in_innova_table(hotel_id="231182")
# print(data)


# insert_data_to_inno_table(local_engine_L1, metadata_local_L1, chunk_size=1000)


# token = authentication_paximum()
# print(token)

# print(type(token))
