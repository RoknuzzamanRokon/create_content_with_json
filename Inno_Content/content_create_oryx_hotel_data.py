from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import json
import os
import random

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

table_main = 'hotel_info_all'


def get_system_id_list(table, column, engine):
    try: 
        query = f"SELECT SystemId FROM {table} WHERE StatusUpdateHotelInfo = 'Done Json';"
        df = pd.read_sql(query, engine)
        # data_all = df[column].tolist()
        # print(len(data_all))
        data = list(set(df[column].tolist()))
        # print(data)
        return data
    except Exception as e:
        print(f"Error fetching column info: {e}")



def get_specifiq_data_from_system_id(table, systemid, engine):
    # SQL query to fetch data for a specific SystemId
    query = f"SELECT * FROM {table} WHERE SystemId = '{systemid}';"
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found for the provided SystemId.")
        return None

    hotel_data = df.iloc[0].to_dict()

    hotel_info = json.loads(hotel_data.get("HotelInfo", "{}"))
    
    createdAt = datetime.now()
    createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
    created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
    timeStamp = int(created_at_dt.timestamp())


    # print("CreatedAt:", created_at_dt)
    # print("Timestamp:", timeStamp)
    # print("HotelInfo:", hotel_info)
    
    # Construct the hotel photo data in the desired format
    hotel_photo_data = [
        {
            "picture_id": None,  
            "title": None,       
            "url": url             
        } for url in hotel_info.get("imageUrls", []) or []
    ]

    hotel_room_amenities = [
        {
            "type": ameList,
            "title": ameList,
            "icon": None
        } for ameList in hotel_info.get("masterRoomAmenities", []) or []
    ]
    
    hotel_amenities = [
        {
            "type": ameList,
            "title": ameList,
            "icon": None
        } for ameList in hotel_info.get("masterHotelAmenities", []) or []
    ]
    
    address_line_1 = hotel_data.get("Address1", None)
    address_line_2 = hotel_data.get("Address2", None)
    hotel_name = hotel_info.get("name", hotel_data.get("HotelName", None))
    # city = hotel_data.get("City", None)
    # postal_code = hotel_data.get("ZipCode", None)
    # state = hotel_info.get("address", {}).get("stateName", None)
    # country = hotel_data.get("CountryName", None)

    address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
    google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None


    specific_data = {
        "created": createdAt_str,
        "timestamp": timeStamp,
        "hotel_id": hotel_data.get("SystemId", None),
        "name": hotel_info.get("name", hotel_data.get("HotelName", None)),
        "name_local": hotel_info.get("name", hotel_data.get("HotelName", None)),
        "hotel_formerly_name": None,
        "destination_code": hotel_data.get("GiDestinationId", None),
        "country_code":  hotel_data.get("CountryCode", None),
        "brand_text": None,
        "property_type": None,
        "star_rating": hotel_info.get("rating", hotel_data.get("Rating", None)),
        "chain": None,
        "brand": None,
        "logo": None,
        "primary_photo": hotel_info.get("imageUrl", hotel_data.get("ImageUrl", None)),
        "review_rating": {
            "source": None,
            "number_of_reviews": None,
            "rating_average": hotel_info.get("tripAdvisorRating", None),
            "popularity_score": None,
        },
        "policies": {
            "checkin": {
                "begin_time": None,
                "end_time": None,
                "instructions": None,
                "special_instructions": None,
                "min_age": None,
            },
            "checkout": {
                "time": None,
            },
            "fees": {
                "optional": None,
            },
            "know_before_you_go": None,
            "pets": None,
            "remark": None,
            "child_and_extra_bed_policy": {
                "infant_age": None,
                "children_age_from": None,
                "children_age_to": None,
                "children_stay_free": None,
                "min_guest_age": None
            },
            "nationality_restrictions": None,
        },
        "address": {
            "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", None)),
            "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", None)),
            "address_line_1": hotel_data.get("Address1", None),
            "address_line_2": hotel_data.get("Address2", None),
            "city": hotel_data.get("City", None),
            "state": hotel_info.get("address", {}).get("stateName", None),
            "country": hotel_data.get("CountryName", None),
            "country_code": hotel_data.get("CountryCode", None),
            "postal_code": hotel_data.get("ZipCode", None),
            "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}",
            "google_map_site_link": google_map_site_link,
            "local_lang": {
                "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", None)),
                "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", None)),
                "address_line_1": hotel_data.get("Address1", None),
                "address_line_2": hotel_data.get("Address2", None),
                "city": hotel_data.get("City", None),
                "state": hotel_info.get("address", {}).get("stateName", None),
                "country": hotel_data.get("CountryName", None),
                "country_code": hotel_data.get("CountryCode", None),
                "postal_code": hotel_data.get("ZipCode", None),
                "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}", 
                "google_map_site_link": google_map_site_link,
            },
            "mapping": {
                "continent_id": None,
                "country_id": hotel_data.get("CountryCode", None),
                "province_id": None,
                "state_id": None,
                "city_id": None,
                "area_id": None
            }
        },
        "contacts": {
            "phone_numbers": [hotel_info.get("contact", {}).get("phoneNo", None)],
            "fax": hotel_info.get("contact", {}).get("faxNo", None),
            "email_address": None,
            "website": hotel_info.get("contact", {}).get("website", hotel_data.get("Website", None))
        },
        "descriptions": [
            {
                "title": None,
                "text": None
            }
        ],
        "room_type": {
            "room_id": None,
            "title": None,
            "title_lang": None,
            "room_pic": None,
            "description": None,
            "max_allowed": {
            "total": None,
            "adults": None,
            "children": None,
            "infant": "n/a"
            },
            "no_of_room": "n/a",
            "room_size": None,
            "bed_type": [
                    {
                    "description": None,
                    "configuration": [
                        {
                        "quantity": None,
                        "size": None,
                        "type": None
                        }
                    ],
                    "max_extrabeds": "n/a"
                    }
                ],
            "shared_bathroom": "n/a"
            },
        "spoken_languages": {
            "type": "spoken_languages",
            "title": "English",
            "icon": "mdi mdi-translate-variant"
            },
        "amenities": hotel_room_amenities,
        "facilities": hotel_amenities,
        "hotel_photo": hotel_photo_data, 
        
        "point_of_interests": [
            {
            "code": None,
            "name": None
            }
        ],
        "nearest_airports": [
            {
            "code": None,
            "name": None
            }
        ],
        "train_stations": [
            {
            "code": None,
            "name": None
            }
        ], 
        "connected_locations": [
            {
            "code": None,
            "name": None
            },
        ],
        "stadiums": [
            {
            "code": None,
            "name": None
            }
        ]
    }


    return specific_data



def initialize_tracking_file(file_path, systemid_list):
    """
    Initializes the tracking file with all SystemIds if it doesn't already exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")


def read_tracking_file(file_path):
    """
    Reads the tracking file and returns a set of remaining SystemIds.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return {line.strip() for line in file.readlines()}


def write_tracking_file(file_path, remaining_ids):
    """
    Updates the tracking file with unprocessed SystemIds.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")


def save_json_files_follow_systemId(folder_path, tracking_file_path, table, column, engine):
    """
    Save JSON files for each SystemId and keep the tracking file updated.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    systemid_list = get_system_id_list(table, column, engine)
    print(f"Total System IDs fetched: {len(systemid_list)}")

    initialize_tracking_file(tracking_file_path, systemid_list)

    remaining_ids = read_tracking_file(tracking_file_path)
    print(f"Remaining System IDs to process: {len(remaining_ids)}")

    while remaining_ids:
        systemid = random.choice(list(remaining_ids))  
        file_name = f"{systemid}.json"
        file_path = os.path.join(folder_path, file_name)

        try:
            if os.path.exists(file_path):
                print(f"File {file_name} already exists. Skipping...........................Ok")
                continue

            data_dict = get_specifiq_data_from_system_id(table, systemid, engine)
            if data_dict is None:
                print(f"No data for SystemId {systemid}. Skipping------------------------No Data")
                remaining_ids.remove(systemid)
                write_tracking_file(tracking_file_path, remaining_ids)
                continue

            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(data_dict, json_file, indent=4)

            print(f"Saved {file_name} in {folder_path}")

            # Remove the processed SystemId from the tracking file immediately
            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)

        except Exception as e:
            print(f"Error processing SystemId {systemid}: {e}")
            continue


folder_path = '../HotelInfo/Oryx'
tracking_file_path = 'tracking_file_for_oryx_content_create.txt'
table_main = 'hotel_info_all'
column_name = 'SystemId'

save_json_files_follow_systemId(folder_path, tracking_file_path, table_main, column_name, engine)