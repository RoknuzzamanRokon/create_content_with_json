from sqlalchemy import create_engine, Table, MetaData, insert, select, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import ast
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Database connection setup
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


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
metadata_server = Table("innova_hotels_main", metadata_server, autoload_with=server_engine)


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
        


        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())


        address_line_1 = hotel_address.get("addressLines", [None, None])[0]
        address_line_2 = hotel_address.get("addressLines", [None, None])[1]
        hotel_name = hotel_data.get("name", None)


        address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"


        seasons = hotel_data.get("seasons", [])
        facility_categories = seasons[0].get("facilityCategories", []) if seasons else []
        
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


        seasons = hotel_data.get("seasons", [])
        media_files = []
        
        if seasons:
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
            "chain": "NULL",
            "brand": "NULL",
            "logo": "NULL",
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
                "nationality_restrictions": "NULL",
            },
            "address": {
                "latitude": hotel_address.get("geolocation", {}).get("latitude", None),
                "longitude": hotel_address.get("geolocation", {}).get("longitude", None),
                "address_line_1": hotel_address.get("addressLines", [None, None])[0], 
                "address_line_2": hotel_address.get("addressLines", [None, None])[1],
                "city": hotel_address.get("city", {}).get("name", None),
                "state": None,
                "country": hotel_data.get("country", {}).get("name", None),
                "country_code": hotel_data.get("country", {}).get("id", None),
                "postal_code": hotel_address.get("zipCode", None),
                "full_address": f"{hotel_address.get("addressLines", [None, None])[0]}, {hotel_address.get("addressLines", [None, None])[1]}",
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": hotel_address.get("geolocation", {}).get("latitude", None),
                    "longitude": hotel_address.get("geolocation", {}).get("longitude", None),
                    "address_line_1": hotel_address.get("addressLines", [None, None])[0], 
                    "address_line_2": hotel_address.get("addressLines", [None, None])[1],
                    "city": hotel_address.get("city", {}).get("name", None),
                    "state": None,
                    "country": hotel_data.get("country", {}).get("name", None),
                    "country_code": hotel_data.get("country", {}).get("id", None),
                    "postal_code": hotel_address.get("zipCode", None),
                    "full_address": f"{hotel_address.get("addressLines", [None, None])[0]}, {hotel_address.get("addressLines", [None, None])[1]}", 
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



def get_hotel_id_list(engine, table):
    with engine.connect() as conn:
        query = select(table.c.HotelId)
        
        df = pd.read_sql(query, conn)
        
        hotel_id_list = df['HotelId'].tolist()
        
    return hotel_id_list





get_provider_ids = get_hotel_id_list(local_engine, metadata_local)

# print(get_provider_ids)



folder_name = "../HotelInfo/Paximum"



for id in get_provider_ids:
    try:
        print(id)

        data = updata_data_in_innova_table(hotel_id=id)
        if data is None:
            continue

        save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
        print(f"Completed Createing Json file for hotel: {id}")
    
    except ValueError:
        print(f"Skipping invalid id: {id}")