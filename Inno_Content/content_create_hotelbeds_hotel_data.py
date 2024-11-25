import requests
import hashlib
import time
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import base64

load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)


table='vervotech_mapping'

class HotelContentHotelBeds:
    def __init__(self, content_hotelbeds):
        load_dotenv()

        self.credentials = {
            'apiKey': os.getenv("HOTELBEDS_API_KEY"),
            'secret': os.getenv("HOTELBEDS_API_SECRET"),
            'base_url': os.getenv("HOTELBEDS_BASE_URL")
        }
        self.hotelbeds = content_hotelbeds

    
    def hotel_api_authentication(self):
        try:
            api_key = self.credentials['apiKey'].strip()
            shared_secret = self.credentials['secret'].strip()
            base_url = self.credentials['base_url'].strip()

            timestamp = str(int(time.time()))
            signature_data = f"{api_key}{shared_secret}{timestamp}"
            signature = hashlib.sha256(signature_data.encode('utf-8')).hexdigest()

            return {
                'apiKey': api_key,
                'secret': shared_secret,
                'signature': signature,
                'base_url': base_url
            }
        except Exception as e:
            print(f"Error in hotel api authentication: {e}")

    def hotel_details(self, hotel_id):
        try:
            hotel_id = hotel_id.strip()

            # print("Loaded API Key:", os.getenv('HOTELBEDS_API_KEY'))
            # print("Loaded Secret Key:", os.getenv('HOTELBEDS_API_SECRET'))

            credentials_data = self.hotel_api_authentication()
            signature = credentials_data['signature']
            api_key = credentials_data['apiKey']
            base_url = credentials_data['base_url']

            url = f"{base_url}/hotel-content-api/1.0/hotels/{hotel_id}/details?language=ENG&useSecondaryLanguage=False"

            headers = {
                "Content-Type": "application/json",
                "Api-key": api_key,
                "X-Signature": signature
            }

            response = requests.get(url, headers=headers, timeout=100)

            # print("Response Status Code:", response.status_code)
            # print("Response Text:", response.text)

            if response.status_code == 200:
                response_data = response.json()
                hotel_data = response_data.get('hotel')
                return hotel_data
            else:
                print(f"Error: Received status code {response.status_code}")
                return None
        except Exception as e:
            print(f"Error in hotel details: {e}")

    
    def iit_hotel_content(self, hotel_id):
        hotel_data = self.hotel_details(hotel_id=hotel_id)

        # print(data)
        # hotel_data = data.get("code", "NULL")
        # print(hotel_data)


        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())


        # Genarate data for google links.
        address_line_1 = hotel_data.get("address", {}).get("content", "NULL")
        address_line_2 = hotel_data.get("zone", {}).get("name")
        hotel_name = hotel_data.get("name", {}).get("content", "NULL")
        long = hotel_data.get("coordinates", {}).get("longitude", "NULL")
        lat = hotel_data.get("coordinates", {}).get("latitude", "NULL")
        city = hotel_data.get("city", {}).get("content", "NULL")
        postal_code = hotel_data.get("postalCode", "NULL")
        state = hotel_data.get("state", {}).get("name", "NULL")
        country = hotel_data.get("country", {}).get("description", {}).get("content", "NULL")

        address_query = f"{address_line_1}, {address_line_2}, {hotel_name}, {long}, {lat}, {city}, {postal_code}, {state}, {country}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"



        # Get phone number.
        phone_number_data = hotel_data.get("phones", [])
        formatted_number = []

        if not phone_number_data:
            formatted_number = "NULL"
        else:
            for value in phone_number_data:
                phone_number_entry = value.get("phoneNumber", "NULL")
                formatted_number.append(phone_number_entry)



        # Email add.
        email_add = hotel_data.get("email", "NULL")

        if not email_add:
            find_mail = "NULL"
        else:
            find_mail = hotel_data.get("email", "NULL")

        # formatted_mail = []

        # if not email_add:
        #     formatted_mail = "NULL"
        # else:
        #     for value in email_add:
        #         email_entry = value.get("email", "NULL")
        #         formatted_mail.append(email_entry)


        # Get Discriptions in supplier data.
        if not hotel_data:
            description_info = "NULL"
        else:
            description_info = {
                "title": "Description",
                "text": hotel_data.get("description", {}).get("content", "NULL")
            }


        # Get room types details.
        get_rooms = hotel_data.get("rooms", [])
        room_type = []

        for room in get_rooms:
            room_type_entry = {
                "room_id": room.get("roomCode", ""),
                "title": room.get("description", ""),
                "title_lang": "", 
                "room_pic": "",  
                "description": room.get("characteristic", {}).get("description", {}).get("content", "NULL"),
                "max_allowed": {
                    "total": room.get("maxPax", 0),
                    "adults": room.get("maxAdults", 0),
                    "children": room.get("maxChildren", 0),
                    "infant": "n/a"  
                },
                "no_of_room": "n/a",  
                "room_size": "", 
                "bed_type": [
                    {
                        "description": room["type"]["description"].get("content", ""),
                        "configuration": [
                            {
                                "quantity": "n/a",  
                                "size": "",     
                                "type": room["type"].get("code", "")
                            }
                        ],
                        "max_extrabeds": "n/a"  
                    }
                ],
                "shared_bathroom": False  
            }
            room_type.append({"room_type": room_type_entry})


        # All facility add here.
        get_facility_data = hotel_data.get("facilities", [])
        formatted_facilities = []

        for facility in get_facility_data:
            facility_entry = {
                "type": facility.get("description", {}).get("content", "NULL"),
                "title": facility.get("description", {}).get("content", "NULL"),
                "icon": "mdi mdi-alpha-f-circle-outline"
            }
            formatted_facilities.append(facility_entry)

        
        # Get all image.
        images_data = hotel_data.get("images", [])
        image_base_url = "http://photos.hotelbeds.com/giata"

        hotel_photos_all = []

        for im_row in images_data:
            hotel_photo_entry = {
                "picture_id": im_row['order'],
                "title": im_row['type']['description']['content'],
                "url": f"{image_base_url}/{im_row['path']}".strip()
            }
            
            hotel_photos_all.append(hotel_photo_entry)
            
        primary_photo_url = hotel_photos_all[0]["url"] if hotel_photos_all else "NULL"

        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_data.get("code", "NUll"),
            "name": hotel_data.get("name", {}).get("content", "NULL"),
            "name_local": hotel_data.get("name", {}).get("content", "NULL"),
            "hotel_formerly_name": hotel_data.get("name", {}).get("content", "NULL"),
            "destination_code": "NULL",
            "country_code":  hotel_data.get("country", {}).get("code", "NULL"),
            "brand_text": "NULL",
            "property_type": hotel_data.get("accommodationType", {}).get("typeDescription", "NULL"),
            "star_rating": hotel_data.get("category", {}).get("description", {}).get("content", "NULL"),
            "chain": hotel_data.get("chain", {}).get("description", {}).get("content", "NULL"),
            "brand": "NULL",
            "logo": "NULL",
            "primary_photo": primary_photo_url,
            
            "review_rating": {
                    "source": "NULL",
                    "number_of_reviews": "NULL",
                    "rating_average": hotel_data.get("ranking", "NULL"),
                    "popularity_score": "NULL"
                },
            "policies": {
                "checkin": {
                    "begin_time": "NULL",
                    "end_time": "NULL",
                    "instructions": "NULL",
                    "special_instructions": "NULL",
                    "min_age":  "NULL",
                    },
                "checkout": {
                    "time": "NULL",
                    },
                "fees": {
                    "optional": "NULL",
                    "mandatory": "NULL",
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
                "latitude": hotel_data.get("coordinates", {}).get("latitude", "NULL"),
                "longitude": hotel_data.get("coordinates", {}).get("longitude", "NULL"),
                "address_line_1": hotel_data.get("address", {}).get("content", "NULL"),
                "address_line_2": hotel_data.get("zone", {}).get("name"),
                "city": hotel_data.get("city", {}).get("content", "NULL"),
                "state": hotel_data.get("state", {}).get("name"),
                "country": hotel_data.get("country", {}).get("description", {}).get("content", "NULL"),
                "country_code": hotel_data.get("country", {}).get("code", "NULL"),
                "postal_code": hotel_data.get("postalCode"),
                "full_address": f"{hotel_data.get("address", {}).get("content", "NULL")}, {hotel_data.get("zone", {}).get("name")}",
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": hotel_data.get("coordinates", {}).get("latitude", "NULL"),
                    "longitude": hotel_data.get("coordinates", {}).get("longitude", "NULL"),
                    "address_line_1": hotel_data.get("address", {}).get("content", "NULL"),
                    "address_line_2": hotel_data.get("zone", {}).get("name"),
                    "city": hotel_data.get("city", {}).get("content", "NULL"),
                    "state": hotel_data.get("state", {}).get("name", "NULL"),
                    "country": hotel_data.get("country", {}).get("description", {}).get("content", "NULL"),
                    "country_code": hotel_data.get("country", {}).get("code", "NULL"),
                    "postal_code": hotel_data.get("postalCode", "NULL"),
                    "full_address": f"{hotel_data.get("address", {}).get("content", "NULL")}, {hotel_data.get("zone", {}).get("name")}",
                    "google_map_site_link": google_map_site_link,
                    },
                "mapping": {
                    "continent_id": "NULL",
                    "country_id": hotel_data.get("country", {}).get("code", "NULL"),
                    "province_id": "NULL",
                    "state_id": "NULL",
                    "city_id": "NULL",
                    "area_id": "NULL"
                    }
                },

            "contacts": {
                "phone_numbers": formatted_number,
                "fax": "NULL",
                "email_address": find_mail,
                "website": "NULL",
                },
            
            "descriptions": description_info,
            "room_type": room_type,
            "spoken_languages": "NULL",
            "amenities": "NULL",
            "facilities": formatted_facilities,
            "hotel_photo": hotel_photos_all, 
            "point_of_interests": "NULL",
            "nearest_airports": "NULL", 
            "train_stations": "NULL",
            "connected_locations": "NULL",
            "stadiums": "NULL"
                
        }

        return specific_data


            

# content_hotelbeds = {}
# hotel_class = HotelContentHotelBeds(content_hotelbeds=content_hotelbeds)

# hotel_id = "459724"
# hotel_data = hotel_class.hotel_details(hotel_id=hotel_id)
# json_hotel_data_format = json.dumps(hotel_data, indent=4) 
# # print(json_hotel_data_format)

# hotel_iit_content = hotel_class.iit_hotel_content(hotel_id=hotel_id)
# json_hotel_iit_content = json.dumps(hotel_iit_content, indent=4)
# print(json_hotel_iit_content)



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


def get_provider_hotel_id_list(engine, table, providerFamily):
    query = f"SELECT ProviderHotelId FROM {table} WHERE ProviderFamily = '{providerFamily}';"
    df = pd.read_sql(query, engine)
    data = df['ProviderHotelId'].tolist()
    return data


providerFamily = "Hotelbeds"
get_provider_ids = get_provider_hotel_id_list(engine=engine, table=table, providerFamily=providerFamily)

# print(get_provider_ids)



folder_name = "../HotelInfo/Hotelbeds"



for id in get_provider_ids:
    try:
        # print(id)

        content_hotelbeds = {}
        hotel_content = HotelContentHotelBeds(content_hotelbeds=content_hotelbeds)

        data = hotel_content.iit_hotel_content(hotel_id=id)
        if data is None:
            continue

        save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
        print(f"Completed Createing Json file for hotel: {id}")
    
    except ValueError:
        print(f"Skipping invalid id: {id}")
