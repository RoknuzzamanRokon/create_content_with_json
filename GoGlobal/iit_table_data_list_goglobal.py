from sqlalchemy import create_engine, select, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json 
import os
from dotenv import load_dotenv
import sys

load_dotenv()

DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=local_engine)
session = Session()
metadata = MetaData()

table_name = Table("innova_hotels_main", metadata, autoload_with=local_engine)




def get_data_from_json_file(file_name):
    folder_path = "D:/content_for_hotel_json/HotelInfo/GoGlobal"
    file_path = os.path.join(folder_path, file_name)

    if not os.path.join(folder_path, file_name):
        raise FileNotFoundError(f"The file '{file_name}' does not exist in '{folder_path}'.")

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except json.JSONDecodeError as e:  
        raise ValueError(f"Error decoding JSON from the file '{file_name}': {e}")



def update_data_for_innova_table(hotel_id):
        hotel_data = get_data_from_json_file(f"{hotel_id}.json")
        facilities_list = hotel_data.get("facilities", [])

        # Amenities groups.
        amenities = {}
        for idx in range(1, 6):
            try:
                amenities[f"Amenities_{idx}"] = facilities_list[idx - 1].get("title", None)
            except (IndexError, AttributeError):
                amenities[f"Amenities_{idx}"] = None

        data = {
            'SupplierCode': 'GoGlobal',
            'HotelId': hotel_data.get("hotel_id", None),
            'City': hotel_data.get("address", {}).get("city", None),
            'PostCode': hotel_data.get("address", {}).get("postal_code", None),
            'Country': hotel_data.get("address", {}).get("country", None),
            'CountryCode': hotel_data.get("country_code", None),
            'HotelName': hotel_data.get("name", None),
            'Latitude': hotel_data.get("address", {}).get("latitude", None),
            'Longitude': hotel_data.get("address", {}).get("longitude", None),
            'PrimaryPhoto':hotel_data.get("primary_photo", None),
            'AddressLine1': hotel_data.get("address", {}).get("address_line_1", None), 
            'AddressLine2': hotel_data.get("address", {}).get("address_line_2", None),
            'HotelReview': hotel_data.get("review_rating", {}).get("number_of_reviews", None),
            'Website': hotel_data.get("contacts", {}).get("website", None),
            'ContactNumber': hotel_data.get("contacts", {}).get("phone_numbers", {})[0] or None,
            'FaxNumber': hotel_data.get("contacts", {}).get("fax", None),
            'HotelStar': hotel_data.get("star_rating", None),
        }

        data.update(amenities)
        return data


def get_hotel_id_list(directory, output_file):
    try:
        json_files = [f[:-5] for f in os.listdir(directory) if f.endswith('.json')]

        with open(output_file, 'w') as file:
            for name in json_files:
                file.write(f"{name}\n")
        print(f"File list has been written to {output_file}")
        return json_files
    except Exception as e:
        print(f"An error occurred: {e}")


def read_tracking_ids(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            tracking_ids = file.read().splitlines()
            return [id.strip() for id in tracking_ids if id.strip()]
    else:
        return[]
    
def write_tracking_ids(file_path, remaining_ids):
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")


def insert_data_to_inno_table_with_tracking(engine, table, tracking_file_path):
    tracking_ids = read_tracking_ids(tracking_file_path)
    print(f"Tracking IDs loaded: {len(tracking_ids)}")

    if not tracking_ids:
        directory = "D:/content_for_hotel_json/HotelInfo/GoGlobal"
        file_path = "goglobal_hotel_id_lsit.txt"
        hotel_list_id = get_hotel_id_list(directory=directory, output_file=file_path)
        write_tracking_ids(tracking_file_path, hotel_list_id)
        tracking_ids = hotel_list_id
        print(f"Tracking file created with {len(tracking_ids)} IDs.")
    
    for hotel_id in tracking_ids.copy():
        try:
            with engine.connect() as conn:
                query = text(f"SELECT COUNT(1) FROM {table.name} WHERE HotelId = :hotel_id AND SupplierCode = 'GoGlobal'")
                result = conn.execute(query, {"hotel_id": hotel_id}).scalar()

            if result > 0:
                print(f"Hotel ID {hotel_id} already exists. Skipping.")
                tracking_ids.remove(hotel_id)
                continue
            
            hotel_data = update_data_for_innova_table(hotel_id=hotel_id)
            if hotel_data:
                try:
                    with engine.connect() as conn:
                        conn.execute(table.insert(), [hotel_data])
                        conn.commit()
                        print(f"Successfully inserted data for Hotel ID {hotel_id}.")
                except Exception as e:
                    print(f"Error inserting data for Hotel ID {hotel_id}: {e}")

            tracking_ids.remove(hotel_id)
            write_tracking_ids(tracking_file_path, tracking_ids)

        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")

    write_tracking_ids(tracking_file_path, tracking_ids)
    print("Processing complete. Updated tracking file.")


engine = local_engine
table = table_name
tracking_file_path = "tracking_file_for_upload_data_in_iit_table.txt"
insert_data_to_inno_table_with_tracking(engine, table, tracking_file_path)
sys.exit()

