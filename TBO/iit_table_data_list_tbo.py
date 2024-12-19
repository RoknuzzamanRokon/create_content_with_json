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
JSON_FOLDER = "D:/Rokon/content_for_hotel_json/HotelInfo/TBO"
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
            'SupplierCode': 'TBO',
            'HotelId': hotel_data.get("hotel_id") or None,
            'City': hotel_data.get("address", {}).get("city") or None,
            'PostCode': hotel_data.get("address", {}).get("postal_code") or None,
            'Country': hotel_data.get("address", {}).get("country") or None,
            'CountryCode': hotel_data.get("country_code") or None,
            'HotelName': hotel_data.get("name") or None,
            'Latitude': hotel_data.get("address", {}).get("latitude") or None,
            'Longitude': hotel_data.get("address", {}).get("longitude") or None,
            'PrimaryPhoto': hotel_data.get("primary_photo") or None,
            'AddressLine1': hotel_data.get("address", {}).get("address_line_1") or None,
            'AddressLine2': hotel_data.get("address", {}).get("address_line_2") or None,
            'HotelReview': hotel_data.get("review_rating", {}).get("number_of_reviews") or None,
            'Website': hotel_data.get("contacts", {}).get("website") or None,
            'ContactNumber': hotel_data.get("contacts", {}).get("phone_numbers") or None,
            'FaxNumber': hotel_data.get("contacts", {}).get("fax") or "NULL",
            'HotelStar': hotel_data.get("star_rating") or None,
        }

        for idx in range(1, 6):
            data[f"Amenities_{idx}"] = facilities[idx - 1].get("title") if len(facilities) >= idx else None

        return data
    except Exception as e:
        print(f"Error processing hotel ID {hotel_id}: {e}")
        return None

def manage_tracking_file(action, ids=None):
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
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(table.insert().values(hotel_data))
            print(f"Inserted data for Hotel ID: {hotel_data['HotelId']}")
    except Exception as e:
        print(f"Error inserting data for Hotel ID {hotel_data['HotelId']}: {e}")

def process_hotels():
    tracking_ids = manage_tracking_file("read")

    if not tracking_ids:
        tracking_ids = [file[:-5] for file in os.listdir(JSON_FOLDER) if file.endswith(".json")]
        manage_tracking_file("write", tracking_ids)
        print(f"Tracking file initialized with {len(tracking_ids)} hotel IDs.")

    while tracking_ids:
        hotel_id = random.choice(tracking_ids) 
        try:
            with engine.connect() as conn:
                exists = conn.execute(text(
                    f"SELECT COUNT(1) FROM {table.name} WHERE HotelId = :hotel_id AND SupplierCode = 'TBO'"
                ), {"hotel_id": hotel_id}).scalar()

            if exists:
                print(f"Hotel ID {hotel_id} already exists. Skipping.")
                tracking_ids.remove(hotel_id)
                continue

            # Extract and insert data
            hotel_data = extract_hotel_data(hotel_id)
            if hotel_data:
                insert_hotel_data(hotel_data)

            # Update tracking file
            tracking_ids.remove(hotel_id)
            manage_tracking_file("write", tracking_ids)

        except Exception as e:
            print(f"Error processing Hotel ID {hotel_id}: {e}")

    print("Hotel data processing completed.")

if __name__ == "__main__":
    process_hotels()
