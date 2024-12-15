import logging
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
import pandas as pd
import ast
import os
from datetime import datetime
import json
import random


logging.basicConfig(
    filename="ratehawk_data_input_local_to_local_table.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine_L2 = create_engine(DATABASE_URL_LOCAL)
Session_L2 = sessionmaker(bind=local_engine_L2)
session_L2 = Session_L2()
metadata_local_L2 = MetaData()
metadata_local_L2.reflect(bind=local_engine_L2)
ratehawk_without_image = Table('ratehawk_without_image', metadata_local_L2, autoload_with=local_engine_L2)
ratehawk_with_image = Table('ratehawk_with_image', metadata_local_L2, autoload_with=local_engine_L2)

local_engine_L1 = create_engine(DATABASE_URL_LOCAL)
Session_L1 = sessionmaker(bind=local_engine_L1)
session_L1 = Session_L1()
metadata_local_L1 = MetaData()
metadata_local_L1.reflect(bind=local_engine_L1)
innova_hotels_main = Table('innova_hotels_main', metadata_local_L1, autoload_with=local_engine_L1)




def create_content_follow_hotel_id(hotel_id):
    try:
        logging.info(f"Starting data transfer for Hotel ID: {hotel_id}...")
        session_L2 = session_L2
        local_engine_L2 = local_engine_L2

        # Fetch data from the `ratehawk_without_image` table
        query_without_image = (
            session_L2.query(ratehawk_without_image)
            .filter(ratehawk_without_image.c.HotelId == hotel_id)
            .statement
        )
        df_without_image = pd.read_sql(query_without_image, local_engine_L2)
        rows_without_image = df_without_image.astype(str).to_dict(orient="records")

        if not rows_without_image:
            logging.warning(f"No data found in `ratehawk_without_image` for Hotel ID: {hotel_id}")
            return []

        # Extract relevant fields from the first table
        filtered_data_without_image = rows_without_image[0]
        keys_to_extract = [
            "address", "id", "kind", "latitude", "longitude", "name",
            "phone", "postal_code", "star_rating", "email"
        ]
        filtered_row_dict = {key: filtered_data_without_image.get(key, None) for key in keys_to_extract}

        # Generate timestamp and Google Maps link
        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        timeStamp = int(createdAt.timestamp())
        address_line_1 = filtered_row_dict.get("address", None)
        hotel_name = filtered_row_dict.get("name", None)
        google_map_site_link = (
            f"http://maps.google.com/maps?q={address_line_1.replace(' ', '+')},{hotel_name.replace(' ', '+')}"
            if address_line_1 and hotel_name else None
        )

        # Fetch data from the `ratehawk_with_image` table
        query_with_image = (
            session_L2.query(ratehawk_with_image)
            .filter(ratehawk_with_image.c.HotelId == hotel_id)
            .statement
        )
        df_with_image = pd.read_sql(query_with_image, local_engine_L2)
        rows_with_image = df_with_image.astype(str).to_dict(orient="records")

        if not rows_with_image:
            logging.warning(f"No data found in `ratehawk_with_image` for Hotel ID: {hotel_id}")

        # Process images and amenity groups
        hotel_data_list = []
        for row_dict in rows_with_image:
            keys_to_extract_part2 = ["id", "images", "region", "amenity_groups"]
            filtered_row_dict_part2 = {key: row_dict.get(key, None) for key in keys_to_extract_part2}

            # Safely parse JSON fields
            region = parse_json_field(filtered_row_dict_part2.get("region", "{}"), {})
            amenity_groups = parse_json_field(filtered_row_dict_part2.get("amenity_groups", "[]"), [])

            # Collect all amenities
            amenity_all = []
            for group in amenity_groups:
                amenities = group.get("amenities", [])
                for amenity in amenities:
                    amenity_all.append({
                        "type": amenity.get("type", None),
                        "title": amenity.get("title", None),
                        "icon": amenity.get("icon", None)
                    })

            # Process hotel images
            images = parse_json_field(filtered_row_dict_part2.get("images", "[]"), [])
            hotel_photos = [
                {
                    "picture_id": None,
                    "title": None,
                    "url": img.replace("t/{size}", "t/x500")
                } for img in images
            ]

            # Create hotel data dictionary
            specific_data = {
                "created": createdAt_str,
                "timestamp": timeStamp,
                "hotel_id": filtered_row_dict.get("id", None),
                "name": filtered_row_dict.get("name", None),
                "name_local": filtered_row_dict.get("name", None),
                "hotel_formerly_name": filtered_row_dict.get("name", None),
                "destination_code": None,
                "country_code": region.get("country_code", None),
                "brand_text": None,
                "property_type": filtered_row_dict.get("kind", None),
                "star_rating": filtered_row_dict.get("star_rating", None),
                "chain": filtered_row_dict.get("hotel_chain", None),
                "brand": None,
                "logo": None,
                "primary_photo": images[0] if images else None,
                "review_rating": {
                    "source": None,
                    "number_of_reviews": None,
                    "rating_average": None,
                    "popularity_score": None,
                },
                "policies": {
                    "checkin": {
                        "begin_time": filtered_row_dict.get("check_in_time", None),
                        "end_time": filtered_row_dict.get("check_out_time", None),
                        "instructions": None,
                        "special_instructions": None,
                        "min_age": None,
                    },
                    "checkout": {
                        "time": filtered_row_dict.get("check_out_time", None),
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
                    "latitude": filtered_row_dict.get("latitude", None),
                    "longitude": filtered_row_dict.get("longitude", None),
                    "address_line_1": filtered_row_dict.get("address", None),
                    "address_line_2": None,
                    "city": region.get("name", None),
                    "state": None,
                    "country": region.get("country_name", None),
                    "country_code": region.get("country_code", None),
                    "postal_code": filtered_row_dict.get("postal_code", None),
                    "full_address": f"{filtered_row_dict.get('address')}",
                    "google_map_site_link": google_map_site_link,
                    "local_lang": {
                        "latitude": filtered_row_dict.get("latitude", None),
                        "longitude": filtered_row_dict.get("longitude", None),
                        "address_line_1": filtered_row_dict.get("address", None),
                        "address_line_2": None,
                        "city": region.get("name", None),
                        "state": None,
                        "country": region.get("country_name", None),
                        "country_code": region.get("country_code", None),
                        "postal_code": filtered_row_dict.get("postal_code", None),
                        "full_address": f"{filtered_row_dict.get('address')}",
                        "google_map_site_link": google_map_site_link,
                    },
                    "mapping": {
                        "continent_id": None,
                        "country_id": None,
                        "province_id": None,
                        "state_id": None,
                        "city_id": None,
                        "area_id": None
                    }
                },
                "contacts": {
                    "phone_numbers": filtered_row_dict.get("phone", None),
                    "fax": None,
                    "email_address": filtered_row_dict.get("email", None),
                    "website": None
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
                    "type": None,
                    "title": None,
                    "icon": None
                },
                "amenities": amenity_all,
                "facilities": None,
                "hotel_photo": hotel_photos, 
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
            hotel_data_list.append(specific_data)

        logging.info(f"Completed data transfer for Hotel ID: {hotel_id} with {len(hotel_data_list)} records.")
        return hotel_data_list

    except Exception as e:
        logging.error(f"Error processing Hotel ID {hotel_id}: {str(e)}", exc_info=True)
        return []


def parse_json_field(value, default):
    """
    Safely parse a JSON field from a string.
    If parsing fails, return the default value.
    """
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return default




def get_all_hotel_id_list_with_supplier(supplier, engine, table):
    try:
        query = select(table.c.HotelId).where(table.c.SupplierCode == supplier)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        hotel_data_list = df['HotelId'].tolist()
        return hotel_data_list
    except Exception as e:
        logging.error(f"Error fetching Hotel IDs for supplier {supplier}: {e}")
        return []




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


def append_to_cannot_find_file(file_path, systemid):
    """
    Appends the SystemId to the 'Cannot find any data' tracking file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")









def save_json_files_follow_systemId(folder_path, tracking_file_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    systemid_list = get_all_hotel_id_list_with_supplier(supplier_name='Ratehawk', engine=local_engine_L1, table=innova_hotels_main)
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
                remaining_ids.remove(systemid)
                write_tracking_file(tracking_file_path, remaining_ids)
                print(f"File {file_name} already exists. Skipping...........................Ok")
                continue

            data_list = create_content_follow_hotel_id(systemid)
            if not data_list:
                print(f"Data not found for Hotel ID: {systemid}. Skipping...")
                continue

            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(data_list, json_file, indent=4)
            print(f"Saved {file_name} in {folder_path}")

            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)

        except Exception as e:
            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)
            print(f"Error processing SystemId {systemid}: {e}")
            continue



# 'D:/content_for_
# hotel_json/HotelInfo/TBO'
folder_path = 'D:/content_for_hotel_json/HotelInfo/Ratehawk'
tracking_file_path = 'tracking_file_for_Ratehawk_content_create.txt'


save_json_files_follow_systemId(folder_path, tracking_file_path)









