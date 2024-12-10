from sqlalchemy import create_engine, Table, MetaData, insert, select, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import ast
import requests
from dotenv import load_dotenv
from datetime import datetime
from io import StringIO 
import random


load_dotenv()

# Database connection setup
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


# DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
# server_engine = create_engine(DATABASE_URL_SERVER)
# Session_1 = sessionmaker(bind=server_engine)
# session_1 = Session_1()


DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(DATABASE_URL_LOCAL)
Session_2 = sessionmaker(bind=local_engine)
session_2 = Session_2()

# metadata_server = MetaData()
metadata_local = MetaData()

# metadata_server.reflect(bind=server_engine)
metadata_local.reflect(bind=local_engine)

paximum_table = Table("Paximum", metadata_local, autoload_with=local_engine)
# metadata_server = Table("innova_hotels_main", metadata_server, autoload_with=server_engine)


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
        # print(f"Authentication token: {token}")

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

        # print(response)

        # print(type(response.text))
        try:
            response_dict = json.loads(response.text)
            # print(response_dict)
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

        # hotel_address = hotel_data.get("address", {}) or {}
        


        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())


        # Address handling
        hotel_address = hotel_data.get("address", {}) or {}
        address_lines = hotel_address.get("addressLines", [])
        address_line_1 = address_lines[0] if len(address_lines) > 0 else None
        address_line_2 = address_lines[1] if len(address_lines) > 1 else None

        # Construct full address
        full_address = ', '.join(filter(None, [address_line_1, address_line_2]))
        google_map_site_link = (
            f"http://maps.google.com/maps?q={full_address.replace(' ', '+')}"
            if full_address
            else None
        )


        seasons = hotel_data.get("seasons", [])
        facility_categories = seasons[0].get("facilityCategories", []) if len(seasons) > 0 else []


        # All facilities list here.
        facilities = []
        for category in facility_categories:
            facilities.extend(category.get("facilities", []))

        hotel_amenities = [
            {
                "type": facility.get("name", "Unknown"),
                "title": facility.get("name", "Unknown"),
                "icon": None
            }
            for facility in facilities
        ]



        # Media files
        media_files = []
        if len(seasons) > 0:
            media_files = seasons[0].get("mediaFiles", [])

        hotel_photo_data = [
            {
                "picture_id": None,
                "title": None,
                "url": media.get("urlFull", "Unknown")
            }
            for media in media_files
        ]


        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_data.get("id", None),
            "name": hotel_data.get("name", None),
            "name_local": hotel_data.get("name", None),
            "hotel_formerly_name": hotel_data.get("name", None),
            "destination_code": None,
            "country_code":  hotel_data.get("country", {}).get("id", None),
            "brand_text": None,
            "property_type": None,
            "star_rating": hotel_data.get("stars", None),
            "chain": None,
            "brand": None,
            "logo": None,
            "primary_photo": hotel_data.get("thumbnailFull", None),
            "review_rating": {
                "source": None,
                "number_of_reviews": None,
                "rating_average": None,
                "popularity_score": None
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
                "latitude": hotel_address.get("geolocation", {}).get("latitude", None),
                "longitude": hotel_address.get("geolocation", {}).get("longitude", None),
                "address_line_1": address_line_1,
                "address_line_2": address_line_2,
                "city": hotel_address.get("city", {}).get("name", None),
                "state": None,
                "country": hotel_data.get("country", {}).get("name", None),
                "country_code": hotel_data.get("country", {}).get("id", None),
                "postal_code": hotel_address.get("zipCode", None),
                "full_address": full_address,
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": hotel_address.get("geolocation", {}).get("latitude", None),
                    "longitude": hotel_address.get("geolocation", {}).get("longitude", None),
                    "address_line_1": address_line_1, 
                    "address_line_2": address_line_2,
                    "city": hotel_address.get("city", {}).get("name", None),
                    "state": None,
                    "country": hotel_data.get("country", {}).get("name", None),
                    "country_code": hotel_data.get("country", {}).get("id", None),
                    "postal_code": hotel_address.get("zipCode", None),
                    "full_address": full_address, 
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
                "phone_numbers": hotel_data.get("phoneNumber", None),
                "fax": hotel_data.get("faxNumber", None),
                "email_address": None,
                "website": hotel_data.get("homePage", None),
            },
            "descriptions": [
                {
                    "title": "Description",
                    "text": hotel_data.get("description", {}).get("text", None)
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
            "amenities": None,
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
    except IndexError as e:
        print(f"IndexError for Hotel ID {hotel_id}: {e}")
        return None  
    except Exception as e:
        print(f"Unexpected error for Hotel ID {hotel_id}: {e}")
        return None



# def save_json_to_folder(data, hotel_id, folder_name):
#     if not os.path.exists(folder_name):
#         os.makedirs(folder_name)
    
#     file_path = os.path.join(folder_name, f"{hotel_id}.json")
#     try:
#         with open(file_path, "w") as json_file:
#             json.dump(data, json_file, indent=4)
#         print(f"Data saved to {file_path}")
#     except TypeError as e:
#         print(f"Serialization error: {e}")
#     except Exception as e:
#         print(f"An error occurred: {e}")



def get_hotel_id_list(engine, table):
    with engine.connect() as conn:
        query = select(table.c.HotelId)
        
        df = pd.read_sql(query, conn)
        
        hotel_id_list = df['HotelId'].tolist()
        
    return hotel_id_list


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











def save_json_files_follow_systemId(folder_path, tracking_file_path, table, engine):
    """
    Save JSON files for each SystemId and keep the tracking file updated.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    systemid_list = get_hotel_id_list(engine, table)
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
                remaining_ids.remove(systemid)
                write_tracking_file(tracking_file_path, remaining_ids)
                continue

            data_dict = updata_data_in_innova_table(systemid)
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


folder_path = '../HotelInfo/Paximum'
tracking_file_path = 'tracking_file_for_paximum_content_create.txt'
# table = paximum_table
# engine = local_engine

save_json_files_follow_systemId(folder_path, tracking_file_path, paximum_table, local_engine)







# get_provider_ids = get_hotel_id_list(local_engine, paximum_table)

# # print(get_provider_ids)



# folder_name = "../HotelInfo/Paximum"



# for id in get_provider_ids:
#     try:
#         print(id)

#         data = updata_data_in_innova_table(hotel_id=id)
#         if data is None:
#             continue

#         save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
#         print(f"Completed Createing Json file for hotel: {id}")
    
#     except ValueError:
#         print(f"Skipping invalid id: {id}")

