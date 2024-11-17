from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import json
import os

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
        query = f"SELECT {column} FROM {table} WHERE StatusUpdateHotelInfo = 'Done Json' AND CountryCode = 'AE';"
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
            "picture_id": "NULL",  
            "title": "NULL",       
            "url": url             
        } for url in hotel_info.get("imageUrls", []) or []
    ]

    hotel_room_amenities = [
        {
            "type": ameList,
            "title": ameList,
            "icon": "NULL"
        } for ameList in hotel_info.get("masterRoomAmenities", []) or []
    ]
    
    hotel_amenities = [
        {
            "type": ameList,
            "title": ameList,
            "icon": "NULL"
        } for ameList in hotel_info.get("masterHotelAmenities", []) or []
    ]
    
    address_line_1 = hotel_data.get("Address1", "NULL")
    address_line_2 = hotel_data.get("Address2", "NULL")
    hotel_name = hotel_info.get("name", hotel_data.get("HotelName", "NULL"))
    # city = hotel_data.get("City", "NULL")
    # postal_code = hotel_data.get("ZipCode", "NULL")
    # state = hotel_info.get("address", {}).get("stateName", "NULL")
    # country = hotel_data.get("CountryName", "NULL")

    address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
    google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"


    specific_data = {
        "created": createdAt_str,
        "timestamp": timeStamp,
        "hotel_id": hotel_data.get("SystemId", "NULL"),
        "name": hotel_info.get("name", hotel_data.get("HotelName", "NULL")),
        "name_local": hotel_info.get("name", hotel_data.get("HotelName", "NULL")),
        "hotel_formerly_name": "NULL",
        "destination_code": hotel_data.get("GiDestinationId", "NULL"),
        "country_code":  hotel_data.get("CountryCode", "NULL"),
        "brand_text": "NULL",
        "property_type": "NULL",
        "star_rating": hotel_info.get("rating", hotel_data.get("Rating", "NULL")),
        "chain": "NULL",
        "brand": "NULL",
        "logo": "NULL",
        "primary_photo": hotel_info.get("imageUrl", hotel_data.get("ImageUrl", "NULL")),
        "review_rating": {
            "source": "NULL",
            "number_of_reviews": "NULL",
            "rating_average": hotel_info.get("tripAdvisorRating", "NULL"),
            "popularity_score": "NULL",
        },
        "policies": {
            "checkin": {
                "begin_time": "NULL",
                "end_time": "NULL",
                "instructions": "NULL",
                "special_instructions": "NULL",
                "min_age": "NULL",
            },
            "checkout": {
                "time": "NULL",
            },
            "fees": {
                "optional": "NULL",
            },
            "know_before_you_go": "NULL",
            "pets": "NULL",
            "remark": "NULL",
            "child_and_extra_bed_policy": {
                "infant_age": "NULL",
                "children_age_from": "NULL",
                "children_age_to": "NULL",
                "children_stay_free": "NULL",
                "min_guest_age": "NULL"
            },
            "nationality_restrictions": "NULL",
        },
        "address": {
            "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", "NULL")),
            "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", "NULL")),
            "address_line_1": hotel_data.get("Address1", "NULL"),
            "address_line_2": hotel_data.get("Address2", "NULL"),
            "city": hotel_data.get("City", "NULL"),
            "state": hotel_info.get("address", {}).get("stateName", "NULL"),
            "country": hotel_data.get("CountryName", "NULL"),
            "country_code": hotel_data.get("CountryCode", "NULL"),
            "postal_code": hotel_data.get("ZipCode", "NULL"),
            "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}",
            "google_map_site_link": google_map_site_link,
            "local_lang": {
                "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", "NULL")),
                "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", "NULL")),
                "address_line_1": hotel_data.get("Address1", "NULL"),
                "address_line_2": hotel_data.get("Address2", "NULL"),
                "city": hotel_data.get("City", "NULL"),
                "state": hotel_info.get("address", {}).get("stateName", "NULL"),
                "country": hotel_data.get("CountryName", "NULL"),
                "country_code": hotel_data.get("CountryCode", "NULL"),
                "postal_code": hotel_data.get("ZipCode", "NULL"),
                "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}", 
                "google_map_site_link": google_map_site_link,
            },
            "mapping": {
                "continent_id": "NULL",
                "country_id": hotel_data.get("CountryCode", "NULL"),
                "province_id": "NULL",
                "state_id": "NULL",
                "city_id": "NULL",
                "area_id": "NULL"
            }
        },
        "contacts": {
            "phone_numbers": [hotel_info.get("contact", {}).get("phoneNo", "NULL")],
            "fax": hotel_info.get("contact", {}).get("faxNo", "NULL"),
            "email_address": "NULL",
            "website": hotel_info.get("contact", {}).get("website", hotel_data.get("Website", "NULL"))
        },
        "descriptions": [
            {
                "title": "NULL",
                "text": "NULL"
            }
        ],
        "room_type": {
            "room_id": "NULL",
            "title": "NULL",
            "title_lang": "NULL",
            "room_pic": "NULL",
            "description": "NULL",
            "max_allowed": {
            "total": "NULL",
            "adults": "NULL",
            "children": "NULL",
            "infant": "n/a"
            },
            "no_of_room": "n/a",
            "room_size": "NULL",
            "bed_type": [
                    {
                    "description": "NULL",
                    "configuration": [
                        {
                        "quantity": "NULL",
                        "size": "NULL",
                        "type": "NULL"
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


def save_json_files_follow_systemId(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    table = 'hotel_info_all'
    column = 'SystemId'

    systemid_list = get_system_id_list(table, column, engine)

    print(f"Total System IDs found: {len(systemid_list)}")

    for systemid in systemid_list:
        file_name = f"{systemid}.json"
        file_path = os.path.join(folder_path, file_name)

        try:
            if os.path.exists(file_path):
                print(f"File {file_name} already exists. Skipping...")
                continue

            try:
                data_dict = get_specifiq_data_from_system_id(table, systemid, engine)

                if data_dict is None:
                    print(f"Data not found for SystemId: {systemid}. Skipping..................................")
                    continue

                with open(file_path, "w") as json_file:
                    json.dump(data_dict, json_file, indent=4)

                print(f"Saved {file_name} in {folder_path}")

            except Exception as e:
                print(f"Error processing data for SystemId {systemid}: {e}")
                continue  

        except Exception as e:
            print(f"Error occurred while checking or creating file for SystemId {systemid}: {e}")
            continue  



folder_path = './HotelInfo/AE'

save_json_files_follow_systemId(folder_path)


