from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import random
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:@localhost/csvdata01_02102024")
engine = create_engine(DATABASE_URL)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

# Define table
table = Table("innova_hotels_main", metadata, autoload_with=engine)

# Constants
JSON_FOLDER = "D:/Rokon/content_for_hotel_json/HotelInfo/GRNConnect"
TRACKING_FILE = "tracking_file_for_upload_data_in_iit_table.txt"

def load_json(file_name):
    """Load a JSON file and return its data."""
    file_path = os.path.join(JSON_FOLDER, f"{file_name}.json")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File '{file_name}.json' not found in {JSON_FOLDER}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON from file '{file_name}.json': {e}")

def extract_hotel_data(hotel_id):
    """Extract hotel data from the JSON file and prepare it for insertion."""
    try:
        hotel_data = load_json(hotel_id)
        facilities = hotel_data.get("facilities", [])

        data = {
            'SupplierCode': 'grnconnect',
            'HotelId': hotel_data.get("hotel_id", "NULL"),
            'City': hotel_data.get("address", {}).get("city", "NULL"),
            'PostCode': hotel_data.get("address", {}).get("postal_code", "NULL"),
            'Country': hotel_data.get("address", {}).get("country", "NULL"),
            'CountryCode': hotel_data.get("country_code", "NULL"),
            'HotelName': hotel_data.get("name", "NULL"),
            'Latitude': hotel_data.get("address", {}).get("latitude", "NULL"),
            'Longitude': hotel_data.get("address", {}).get("longitude", "NULL"),
            'PrimaryPhoto': hotel_data.get("primary_photo", "NULL"),
            'AddressLine1': hotel_data.get("address", {}).get("address_line_1", "NULL"),
            'AddressLine2': hotel_data.get("address", {}).get("address_line_2", "NULL"),
            'HotelReview': hotel_data.get("review_rating", {}).get("number_of_reviews", "NULL"),
            'Website': hotel_data.get("contacts", {}).get("website", "NULL"),
            'ContactNumber': hotel_data.get("contacts", {}).get("phone_numbers", "NULL"),
            'FaxNumber': hotel_data.get("contacts", {}).get("fax", "NULL"), 
            'HotelStar': hotel_data.get("star_rating", "NULL"),
        }

        for idx in range(1, 6):
            data[f"Amenities_{idx}"] = facilities[idx - 1].get("title") if len(facilities) >= idx else None

        return data
    except Exception as e:
        print(f"Error processing hotel ID {hotel_id}: {e}")
        return None

def manage_tracking_file(action, ids=None):
    """Manage the tracking file for hotel processing."""
    if action == "read":
        if os.path.exists(TRACKING_FILE):
            with open(TRACKING_FILE, "r", encoding="utf-8") as file:
                return [line.strip() for line in file if line.strip()]
        return []
    elif action == "write" and ids is not None:
        with open(TRACKING_FILE, "w", encoding="utf-8") as file:
            file.write("\n".join(ids) + "\n")
    else:
        raise ValueError("Invalid action or missing IDs for tracking file management.")

def insert_hotel_data(hotel_data):
    """Insert hotel data into the database."""
    try:
        # Ensure FaxNumber and other critical fields are not None
        hotel_data['FaxNumber'] = hotel_data['FaxNumber'] or "Not Provided"
        
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(table.insert().values(hotel_data))
            # print(f"Inserted data for Hotel ID: {hotel_data['HotelId']}")
    except Exception as e:
        print(f"Error inserting data for Hotel ID {hotel_data['HotelId']}: {e}")

def process_hotels():
    tracking_ids = manage_tracking_file("read")

    while tracking_ids:
        hotel_id = random.choice(tracking_ids)
        try:
            # print(f"Processing Hotel ID: {hotel_id}")  
            
            with engine.connect() as conn:
                exists = conn.execute(text(
                    f"SELECT COUNT(1) FROM {table.name} WHERE HotelId = :hotel_id AND SupplierCode = 'grnconnect'"
                ), {"hotel_id": hotel_id}).scalar()
                # print(f"Hotel ID {hotel_id} exists in DB: {exists}")

            if exists:
                print(f"Hotel ID {hotel_id} already exists. Skipping.")
                tracking_ids.remove(hotel_id)
                continue

            hotel_data = extract_hotel_data(hotel_id)
            # print(f"Extracted Data for {hotel_id}: {hotel_data}")
            
            if hotel_data:
                insert_hotel_data(hotel_data)
                print(f"Successfully inserted Hotel ID {hotel_id}")

                tracking_ids.remove(hotel_id)
                manage_tracking_file("write", tracking_ids)

        except Exception as e:
            print(f"Error processing Hotel ID {hotel_id}: {e}")

    print("Hotel data processing completed.")


if __name__ == "__main__":
    process_hotels()
