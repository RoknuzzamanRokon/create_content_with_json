import requests
import xmltodict
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import random


load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)




gtrs_api_key = os.getenv("AGODA_API_KEY")


def get_xml_to_json_data_for_agoda(api_key, hotel_id):
    url = f"https://affiliatefeed.agoda.com/datafeeds/feed/getfeed?apikey={api_key}&mhotel_id={hotel_id}&feed_id=19"
    response = requests.get(url)

    if response.status_code == 200:
        xml_data = response.content
        data_dict = xmltodict.parse(xml_data)

        # Ensure "Hotel_feed_full" exists in the parsed data
        hotel_feed_full = data_dict.get("Hotel_feed_full")
        if hotel_feed_full is None:
            print(f"Skipping hotel {hotel_id} as 'Hotel_feed_full' is not found.")
            return None

        hotel_data = hotel_feed_full.get("hotels", {}).get("hotel", {})
        
        if not hotel_data.get("hotel_id"):
            print(f"Skipping hotel {hotel_id} as 'hotel_id' is not found.")
            return None
        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())

        # Genarate data for google links.
        address_line_1 = hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_1", None)
        address_line_2 = hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_2", None)
        hotel_name = hotel_data.get("hotel_name", None)
        city = hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("city", None)
        postal_code = hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("postal_code", None)
        country = hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("country", None)

        address_query = f"{address_line_1}, {address_line_2}, {hotel_name}, {city}, {postal_code}, {country}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None


        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_data.get("hotel_id", None),
            "name": hotel_data.get("hotel_name", None),
            "name_local": hotel_data.get("translated_name", None),
            "hotel_formerly_name": hotel_data.get("hotel_formerly_name", None),
            "destination_code": None,
            "country_code": None,
            "brand_text": None,
            "property_type": hotel_data.get("accommodation_type", None),
            "star_rating": hotel_data.get("star_rating", None),
            "chain": None,
            "brand": None,
            "logo": None,
            "primary_photo": None,
            "review_rating": {
                "source": None,
                "number_of_reviews": hotel_data.get("number_of_reviews", None),
                "rating_average": hotel_data.get("rating_average", None),
                "popularity_score": hotel_data.get("popularity_score", None),
            },
            "policies": {
                "check_in": {
                    "begin_time": None,
                    "end_time": None,
                    "instructions": None,
                    "min_age": None,
                },
                "checkout": {
                    "time": None,
                },
                "fees": {
                    "optional": None,
                },
                "know_before_you_go": None,
                "pets": [
                    "Pets not allowed"
                ],
                "remark": None,
                "child_and_extra_bed_policy": {
                    "infant_age": hotel_data.get("child_and_extra_bed_policy", {}).get("infant_age", None),
                    "children_age_from": hotel_data.get("child_and_extra_bed_policy", {}).get("children_age_from", None),
                    "children_age_to": hotel_data.get("child_and_extra_bed_policy", {}).get("children_age_to", None),
                    "children_stay_free": hotel_data.get("child_and_extra_bed_policy", {}).get("children_stay_free", None),
                    "min_guest_age": hotel_data.get("child_and_extra_bed_policy", {}).get("min_guest_age", None)
                },
                "nationality_restrictions": hotel_data.get("nationality_restrictions", None),
            },
            "address": {
                "latitude": hotel_data.get("latitude", None),
                "longitude": hotel_data.get("longitude", None),
                "address_line_1": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_1", None),
                "address_line_2": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_2", None),
                "city": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("city", None),
                "state": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("state", None),
                "country": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("country", None),
                "country_code": None,
                "postal_code": hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("postal_code", None),
                "full_address": f"{hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_1", None)}, {hotel_feed_full.get("addresses", {}).get("address", [{}])[0].get("address_line_2", None)}",
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": hotel_data.get("latitude", None),
                    "longitude": hotel_data.get("longitude", None),
                    "address_line_1": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("address_line_1", None),
                    "address_line_2": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("address_line_2", None),
                    "city": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("city", None),
                    "state": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("state", None),
                    "country": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("country", None),
                    "country_code": None,
                    "postal_code": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("postal_code", None),
                    "full_address": f"{hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("address_line_1", None)}, {hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("address_line_2", None)}",
                    "google_map_site_link": google_map_site_link,
                },
                "mapping": {
                    "continent_id": None,
                    "country_id": hotel_feed_full.get("addresses", {}).get("address", [{}])[1].get("country", None),
                    "province_id": None,
                    "state_id": None,
                    "city_id": None,
                    "area_id": None
                }
            },
            "contacts": {
                "phone_numbers": [],
                "fax": None,
                "email_address": None,
                "website": None
            },
            "descriptions": [
                {
                    "title": None,
                    "text": None
                }
            ],
            "room_type": [],
            "spoken_languages": [],
            "amenities": [],
            "facilities": [],
            "hotel_photo": [],
            "point_of_interests": [],
            "nearest_airports": [],
            "train_stations": [],
            "connected_locations": [],
            "stadiums": []    
        }

        # Room types processing
        if hotel_feed_full.get("roomtypes") is not None:
            room_types = hotel_feed_full.get("roomtypes", {}).get("roomtype", [])
            for room in room_types:
                if isinstance(room, dict):
                    room_data = {
                        "room_id": room.get("hotel_room_type_id", None),
                        "title": room.get("standard_caption", None),
                        "title_lang": room.get("standard_caption", None),
                        "room_pic": room.get("hotel_room_type_picture", None),
                        "description": None,
                        "max_allowed": {
                            "total": int(room.get("max_occupancy_per_room", 0)),
                            "adults": int(room.get("max_occupancy_per_room", 0)),
                            "children": None,
                            "infant": room.get("max_infant_in_room", None),
                        },
                        "no_of_room": room.get("no_of_room", None),
                        "room_size": room.get("size_of_room", 0),
                        "bed_type": [
                            {
                                "description": room.get("bed_type", None),
                                "configuration": [],
                                "max_extrabeds": room.get("max_extrabeds", None),
                            }
                        ],
                        "shared_bathroom": room.get("shared_bathroom", None),
                    }
                    specific_data["room_type"].append(room_data)
                else:
                    print(f"Skipping room entry as it is not a dictionary: {room}")
        else:
            print(f"Skipping hotel {hotel_id} as 'room_types' is not found")

        # Facilities processing
        facilities_types = hotel_feed_full.get("facilities")
        if facilities_types is None:
            print(f"Skipping hotel {hotel_id} as 'facilities' is ------------------------------------ not found.")
        else:
            facilities_types = facilities_types.get("facility", [])
            if isinstance(facilities_types, list):
                for facility in facilities_types:
                    if isinstance(facility, dict):
                        facilities_data = {
                            "type": facility.get("property_name", None),
                            "title": facility.get("property_group_description", None),
                            "icon": facility.get("property_translated_name", None)
                        }
                        specific_data["facilities"].append(facilities_data)
                    else:
                        print(f"Skipping facility entry as it is not a dictionary: {facility}")
            else:
                print(f"No facilities found for hotel {hotel_id}")


        # Hotel photo processing
        if hotel_feed_full.get("pictures") is not None:
            hotel_photo_data = hotel_feed_full["pictures"].get("picture", [])
            
            # Initialize storage for photos
            specific_data["primary_photo"] = None  
            specific_data["hotel_photo"] = []     

            for index, photo in enumerate(hotel_photo_data):
                if isinstance(photo, dict):
                    photo_data = {
                        "picture_id": photo.get("picture_id", None),
                        "title": photo.get("caption", None),
                        "url": photo.get("URL", None)
                    }
                    
                    if index == 0:
                        specific_data["primary_photo"] = photo.get("URL", None)
                    
                    specific_data["hotel_photo"].append(photo_data)
                else:
                    print(f"Skipping photo entry as it is not a dictionary: {photo}")

        return specific_data
    else:
        print(f"Error fetching data from API for hotel {hotel_id}: Status code {response.status_code}")
        return None


def save_json_to_folder(data, hotel_id, folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    file_path = os.path.join(folder_name, f"{hotel_id}.json")
    try:
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data saved to {file_path}")
    except TypeError as e:
        print(f"Serialization error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

        
# data = get_xml_to_json_data_for_agoda(api_key=gtrs_api_key, hotel_id=15281267)
# save_json_to_folder(data, hotel_id=15281267, folder_name="Agoda")



def get_vervotech_id(engine, table, providerFamily):
    query = f"SELECT ProviderHotelId FROM {table} WHERE ProviderFamily = '{providerFamily}';"
    df = pd.read_sql(query, engine)
    data = df['ProviderHotelId'].tolist()
    return data










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











def save_json_files_follow_systemId(folder_path, tracking_file_path, engine):
    """
    Save JSON files for each SystemId and keep the tracking file updated.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    table = "vervotech_mapping"
    providerFamily = "Agoda"

    systemid_list = get_vervotech_id(engine, table, providerFamily)
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

            data_dict = get_xml_to_json_data_for_agoda(api_key=gtrs_api_key, hotel_id=systemid)
            # print(data_dict)
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


folder_path = '../HotelInfo/Agoda'
tracking_file_path = 'tracking_file_for_agoda_content_create.txt'
# table = paximum_table
# engine = local_engine

save_json_files_follow_systemId(folder_path, tracking_file_path, engine)




















# table = "vervotech_mapping"
# providerFamily = "Agoda"
# ids = get_vervotech_id(engine=engine, table=table, providerFamily=providerFamily)

# folder_name = '../HotelInfo/Agoda'

# for id in ids:
#     try:
#         hotel_id = int(id)  
#         data = get_xml_to_json_data_for_agoda(api_key=gtrs_api_key, hotel_id=hotel_id)

#         if data is None:
#             continue  

#         save_json_to_folder(data=data, hotel_id=hotel_id, folder_name=folder_name)
#         print(f"Completed creating JSON file for hotel {hotel_id}")

#     except ValueError:
#         print(f"Skipping invalid id: {id} (cannot convert to int)")








