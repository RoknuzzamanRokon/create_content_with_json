from sqlalchemy import create_engine
import requests
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import random

# Load environment variables
load_dotenv()

grnconnect_api_key = os.getenv("GRNCONNECT_API_KEY")
grnconnect_base_url = os.getenv("GRNCONNECT_BASE_URL")
# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)


table='vervotech_mapping'


class GRNConnectAPI:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url

    def fetch_data(self, endpoint, params=None):
        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "application/gzip",
                "api-key": self.api_key
            }

            response = requests.get(endpoint, headers=headers, params=params)

            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "error": f"Failed to fetch data. HTTP Status: {response.status_code}",
                    "response": response.text
                }

        except Exception as e:
            return {"error": str(e)}

    def hotel_genaral_data(self, hotel_code):
        endpoint = f"{self.base_url}/api/v3/hotels?hcode={hotel_code}&version=2.0"
        return self.fetch_data(endpoint)
        
    def hotel_images(self, hotel_code):
        endpoint = f"{self.base_url}/api/v3/hotels/{hotel_code}/images?version=2.0"
        return self.fetch_data(endpoint)

    def iit_hotel_content(self, hotel_code):
        try:
            genaral_data = self.hotel_genaral_data(hotel_code)
            total = genaral_data.get("total", None)
            if total == 1:
                hotel_data = genaral_data.get("hotels", [])[0]
                

                createdAt = datetime.now()
                createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
                created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
                timeStamp = int(created_at_dt.timestamp())
        
        
        
                # Genarate data for google links.
                address_line_1 = hotel_data.get("address", None)
                address_line_2 = None
                hotel_name = hotel_data.get("name", None)
                # city = hotel_data.get("address", {}).get("city", "NULL")
                postal_code = hotel_data.get("postal_code", None)
                country = hotel_data.get("country", None)
        
                address_query = f"{address_line_1}, {address_line_2}, {hotel_name}, {postal_code}, {country}"
                google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None
        

                # All facilitiys list here.
                facilities = hotel_data.get("facilities", "") or None
                if not facilities:
                    hotel_facilities = {
                        "type": None,
                        "title": None,
                        "icon": None
                    }
                else:
                    facilities_list = facilities.split(" ; ")
                    hotel_facilities = []
        
                    for facility in facilities_list:
                        facility_entry = {
                            "type": facility,
                            "title": facility,
                            "icon": "mdi mdi-alpha-f-circle-outline"
                        }
                        hotel_facilities.append(facility_entry)

                
                # Description list here.
                description = hotel_data.get("description", None)
                if not description:
                    descriptions_data = None
                else:
                    descriptions_data = [
                        {
                            "title": "Description",
                            "text": hotel_data.get("description", None)
                        } 
                    ]

                
                # Images all list here.   
                hotel_images_data = self.hotel_images(hotel_code)
                images = hotel_images_data.get("images", {}).get("regular", [])
                hotel_images = []
                primary_photo = None
                
                if not images:
                    hotel_images = {
                        "picture_id": None,
                        "title": None,
                        "url": None
                    }
                else:
                    primary_photo = images[0].get("url", None)
                    for image in images:
                        image_entry = {
                            "picture_id": None,
                            "title": image.get("caption", ""),
                            "url": image.get("url", "")
                        }
                        hotel_images.append(image_entry)


                
                specific_data = {
                    "created": createdAt_str,
                    "timestamp": timeStamp,
                    "hotel_id": hotel_data.get("code", None),
                    "name": hotel_data.get("name", None),
                    "name_local": hotel_data.get("name", None),
                    "hotel_formerly_name": hotel_data.get("name", None),
                    "destination_code": None,
                    "country_code":  hotel_data.get("country", None),
                    "brand_text": None,
                    "property_type": hotel_data.get("acc_name", None),
                    "star_rating": None,
                    "chain": hotel_data.get("chain_name", None),
                    "brand": None,
                    "logo": None,
                    "primary_photo": primary_photo,
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
                            "min_age":  None,
                            },
                        "checkout": {
                            "time": None,
                            },
                        "fees": {
                            "optional": None,
                            "mandatory": None,
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
                        "latitude": hotel_data.get("latitude", None),
                        "longitude": hotel_data.get("longitude", None),
                        "address_line_1": hotel_data.get("address", None),
                        "address_line_2": None,
                        "city": None,
                        "state": None,
                        "country": None,
                        "country_code": hotel_data.get("country", None),
                        "postal_code": hotel_data.get("postal_code", None),
                        "full_address": f"{hotel_data.get("address", None)}",
                        "google_map_site_link": google_map_site_link,
                        "local_lang": {
                            "latitude": hotel_data.get("latitude", None),
                            "longitude": hotel_data.get("longitude", None),
                            "address_line_1": hotel_data.get("address", None),
                            "address_line_2": None,
                            "city": None,
                            "state": None,
                            "country": None,
                            "country_code": hotel_data.get("country", None),
                            "postal_code": hotel_data.get("postal_code", None),
                            "full_address": f"{hotel_data.get("address", None)}",
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
                        "phone_numbers": hotel_data.get("Phone", None),
                        "fax": hotel_data.get("fax", None),
                        "email_address": hotel_data.get("email", None),
                        "website": hotel_data.get("website", None),
                        },
                    
                    "descriptions": descriptions_data,
                    "room_type": [],
                    "spoken_languages": [],
                    "amenities": [],
                    "facilities": hotel_facilities,
                    "hotel_photo": hotel_images, 
        
                    "point_of_interests": [
                        {
                        "code": "NULL",
                        "name": "NULL"
                        }
                    ],
                    "nearest_airports": [
                        {
                        "code": "NULL",
                        "name": "NULL"
                        }
                    ],
                    "train_stations": [
                        {
                        "code": "NULL",
                        "name": "NULL"
                        }
                    ], 
                    "connected_locations": [
                        {
                        "code": "NULL",
                        "name": "NULL"
                        },
                    ],
                    "stadiums": [
                        {
                        "code": "NULL",
                        "name": "NULL"
                        }
                    ]
                        
                }
        
        
                return specific_data
            else:
                return "Cannot find."
        except Exception as e:
            print(f"{e}")
        



# Instantiate the API client
# grnconnect = GRNConnectAPI(api_key=grnconnect_api_key, base_url=grnconnect_base_url)

# Example usage
# hotel_code = "1492043"  
# hotel_face_data = grnconnect.hotel_genaral_data(hotel_code)
# hotel_images = grnconnect.hotel_images(hotel_code)
# content_main = grnconnect.iit_hotel_content(hotel_code)

# Print the response in a formatted JSON
# print(json.dumps(hotel_face_data, indent=4))
# print(json.dumps(hotel_images, indent=4))
# print(json.dumps(content_main, indent=4))



def get_provider_hotel_id_list(engine, table, providerFamily):
    query = f"SELECT DISTINCT ProviderHotelId FROM {table} WHERE ProviderFamily = '{providerFamily}';"
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


def append_to_cannot_find_file(file_path, systemid):
    """
    Appends the SystemId to the 'Cannot find any data' tracking file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")





def save_json_files_follow_systemId(folder_path, tracking_file_path, cannot_find_file_path, engine):
    """
    Save JSON files for each SystemId and keep the tracking file updated.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    table = "vervotech_mapping"
    providerFamily = "GRNConnect"

    systemid_list = get_provider_hotel_id_list(engine=engine, table=table, providerFamily=providerFamily)
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

            grnconnect = GRNConnectAPI(api_key=grnconnect_api_key, base_url=grnconnect_base_url)

            data_dict = grnconnect.iit_hotel_content(hotel_code=systemid)

            if data_dict == "Cannot find.":
                print(f"No data for SystemId {systemid}. Logging to 'Cannot find any data' list.")
                append_to_cannot_find_file(cannot_find_file_path, systemid)
                remaining_ids.remove(systemid)
                write_tracking_file(tracking_file_path, remaining_ids)
                continue

            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(data_dict, json_file, indent=4)

            print(f"Saved {file_name} in {folder_path}")

            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)

        except Exception as e:
            print(f"Error processing SystemId {systemid}: {e}")
            continue

    try:
        cannot_find_ids = read_tracking_file(cannot_find_file_path)
        remaining_ids = read_tracking_file(tracking_file_path)
        updated_ids = remaining_ids - cannot_find_ids
        write_tracking_file(tracking_file_path, updated_ids)
        print(f"Updated tracking file, removed IDs in 'Cannot find any data' list.")
    except Exception as e:
        print(f"Error updating tracking file: {e}")

folder_path = '../HotelInfo/GRNConnect'
tracking_file_path = 'tracking_file_for_GRNConnect_content_create.txt'
cannot_find_file_path = 'cannot_find_data_list.txt'

save_json_files_follow_systemId(folder_path, tracking_file_path, cannot_find_file_path, engine)
