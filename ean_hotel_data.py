import requests
import hashlib
import time
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

table='vervotech_mapping'


class HotelContentEAN:
    def __init__(self, content_vervotech):
        load_dotenv()

        self.credentials = {
            'api_key': os.getenv('EAN_API_KEY'),
            'api_secret': os.getenv('EAN_API_SECRET'),
            'base_url': os.getenv('EAN_BASE_UEL')
        }
        self.vervotech = content_vervotech


    def hotel_api_authentication(self):
        try:
            api_key = self.credentials['api_key'].strip()
            api_secret = self.credentials['api_secret'].strip()
            base_url = self.credentials['base_url'].strip()

            # print(f'Base url {base_url}')

            timestamp = int(time.time())

            # print(f'Get current time in seconds: {timestamp}')

            signature = hashlib.sha512(f'{api_key}{api_secret}{timestamp}'.encode('utf-8')).hexdigest()


            auth_header = f"EAN APIKey={api_key},Signature={signature},timestamp={timestamp}"

            data = {
                'apiKey': api_key,
                'secret': api_secret,
                'timestamp': timestamp,
                'authHeader': auth_header,
                'base_url': base_url
            }
            return data
        except Exception as e:
            print(f"Error in hotel api authentication: {e}")


    def hotel_details(self, hotel_id):
        try:
            get_fields = {
                'language': 'en-US',
                'supply_source': 'expedia',
                'property_id': hotel_id
            }
            
            url_generate = '&'.join([f"{key}={value}" for key, value in get_fields.items()])

            creadentials_data = self.hotel_api_authentication()
            auth_header = creadentials_data['authHeader']
            base_url = creadentials_data['base_url']

            curl_url = f"{base_url}/v3/properties/content?{url_generate}"
            
            headers = {
                "Accept": "application/json",
                "Authorization": auth_header,
                "Content-Type": "application/json"

            }
            response = requests.get(curl_url, headers=headers, timeout=100)

            if response.status_code == 200:
                response_data = response.json()
                # json_response = json.dumps(response_data, indent=4)
                # return json_response
                hotel_data = response_data.get(hotel_id, {})
                return hotel_data
            else:
                print(f"Error: Failed to restrieve data, status code: {response.status_code}")
                print(response.taxt)
                return {}
        
        except Exception as e:
            print(f"Error in hotel_details: {e}")


    def iit_hotel_content(self, hotel_id):

        # df = pd.read_html()
        
        hotel_data = self.hotel_details(hotel_id)


        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())




        # Genarate data for pets.
        attributes_data = hotel_data.get("attributes", {})
        pets_data = [
            {
                "id": pet_info["id"],
                "name": pet_info["name"]
            } for pet_info in attributes_data.get("pets", {}).values()
        ]

        descriptions = hotel_data.get("descriptions", {})
        descriptions_data = [
            {
                "title": title,
                "text": text
            } for title, text in descriptions.items()
        ]



        # Genarate data for google links.
        address_line_1 = hotel_data.get("address", {}).get("line_1", "NULL"),
        address_line_2 = hotel_data.get("address", {}).get("line_2", "NULL"),
        hotel_name = hotel_data.get("name", "NUll"),
        # city = hotel_data.get("City", "NULL")
        # postal_code = hotel_data.get("ZipCode", "NULL")
        # state = hotel_info.get("address", {}).get("stateName", "NULL")
        # country = hotel_data.get("CountryName", "NULL")

        address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"



        # Genarate data for room details.
        room_ids = hotel_data.get("rooms", {})
        structured_room_types = []

        for room_id, room_info in room_ids.items():
            bed_types = []
            for bed_group in room_info.get("bed_groups", {}).values():
                bed_type_entry = {
                    "description": bed_group["description"],
                    "configuration": [
                        {"quantity": config["quantity"], "size": config["size"], "type": config["type"]}
                        for config in bed_group.get("configuration", [])
                    ],
                    "max_extrabeds": bed_group.get("max_extrabeds", "n/a")
                }
                bed_types.append(bed_type_entry)

            # Retrieve images with fallback options
            images = room_info.get("images", [])
            if images:
                first_image_links = images[0].get("links", {})
                image_url = (
                    first_image_links.get("1000px", {}).get("href")
                    or first_image_links.get("350px", {}).get("href")
                    or "No Image Available"
                )
            else:
                image_url = "No Image Available"

            room_data = {
                "room_id": room_info["id"],
                "title": room_info["name"],
                "title_lang": room_info["name"],
                "room_pic": image_url,  
                "description": room_info["descriptions"].get("overview", "No description available"),
                "max_allowed": {
                    "total": room_info["occupancy"]["max_allowed"]["total"],
                    "adults": room_info["occupancy"]["max_allowed"]["adults"],
                    "children": room_info["occupancy"]["max_allowed"]["children"],
                    "infant": "n/a"
                },
                "no_of_room": "n/a",
                "room_size": room_info.get("area", {}).get("square_feet", "n/a"),
                "bed_type": bed_types,
                "shared_bathroom": False
            }

            structured_room_types.append(room_data)
             



        # Spoking language
        spoken_languages = hotel_data.get("spoken_languages", {})
        transformed_spoken_languages = []

        for key, value in spoken_languages.items():
            language_entry = {
                "type": "spoken_languages",
                "title": value["name"],
                "icon": "mdi mdi-translate-variant"
            }
            transformed_spoken_languages.append(language_entry)
            


        # Retrieve amenities data
        amenities = hotel_data.get("amenities", {})
        hotel_room_amenities = []

        for key, value in amenities.items():
            amenity_entry = {
                "type": value["name"],
                "title": value["name"],
                "icon": "mdi mdi-alpha-f-circle-outline"
            }
            hotel_room_amenities.append(amenity_entry)



        # Retrieve genaral attributes data for facility.
        general_attributes = hotel_data.get("attributes", {}).get("general", {})
        hotel_amenities = []
        
        for key, value in general_attributes.items():
            facility_entry = {
                "type": value["name"],
                "title": value["name"],
                "icon": "mdi mdi-alpha-f-circle-outline"
            }
            hotel_amenities.append(facility_entry)



        # Retrieve the images data.
        images = hotel_data.get("images", [])
        hotel_photo_data = []

        for image in images:
            image_entry = {
                "picture_id": image["category"],
                "title": image["caption"],
                "url": image["links"].get("1000px", {}).get("href", "No Image Available")
            }
            hotel_photo_data.append(image_entry)

        if "images" in hotel_data and hotel_data["images"]:
            primary_photo = hotel_data["images"][0].get("links", {}).get("1000px", {}).get("href")
        else:
            primary_photo = None

        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_data.get("property_id", "NUll"),
            "name": hotel_data.get("name", "NUll"),
            "name_local": hotel_data.get("name", "NUll"),
            "hotel_formerly_name": hotel_data.get("name", "NUll"),
            "destination_code": "NULL",
            "country_code":  hotel_data.get("address", {}).get("country_code", "NULL"),
            "brand_text": "NULL",
            "property_type": hotel_data.get("category", {}).get("name", "NULL"),
            "star_rating": hotel_data.get("ratings", {}).get("property", {}).get("rating", "NULL"),
            "chain": hotel_data.get("chain", {}).get("name", "NULL"),
            "brand": hotel_data.get("brand", {}).get("name", "NULL"),
            "logo": "NULL",
            "primary_photo": primary_photo,
            
            "review_rating": {
                    "source": "Expedia.com",
                    "number_of_reviews": hotel_data.get("ratings", {}).get("guest", {}).get("count", "NULL"),
                    "rating_average": hotel_data.get("rank", "NULL"),
                    "popularity_score": hotel_data.get("ratings", {}).get("guest", {}).get("overall", "NULL")
                },
            "policies": {
                "checkin": {
                    "begin_time": hotel_data.get("checkin", {}).get("begin_time", "NULL"),
                    "end_time": hotel_data.get("checkin", {}).get("end_time", "NULL"),
                    "instructions": hotel_data.get("checkin", {}).get("instructions", "NULL"),
                    "special_instructions": hotel_data.get("checkin", {}).get("special_instructions", "NULL"),
                    "min_age":  hotel_data.get("checkin", {}).get("min_age", "NULL"),
                    },
                "checkout": {
                    "time": hotel_data.get("checkout", {}).get("time", "NULL"),
                    },
                "fees": {
                    "optional": hotel_data.get("fees", {}).get("optional", "NULL"),
                    "mandatory": hotel_data.get("fees", {}).get("mandatory", "NULL"),
                    },
                "know_before_you_go": hotel_data.get("policies", {}).get("know_before_you_go", "NULL"),
                "pets": pets_data,
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
                "latitude": hotel_data.get("location", {}).get("coordinates", {}).get("latitude", "NULL"),
                "longitude": hotel_data.get("location", {}).get("coordinates", {}).get("longitude", "NULL"),
                "address_line_1": hotel_data.get("address", {}).get("line_1", "NULL"),
                "address_line_2": hotel_data.get("address", {}).get("line_2", "NULL"),
                "city": hotel_data.get("address", {}).get("city", "NULL"),
                "state": hotel_data.get("address", {}).get("state_province_name", "NULL"),
                "country": hotel_data.get("address", {}).get("country_code", "NULL"),
                "country_code": hotel_data.get("address", {}).get("country_code", "NULL"),
                "postal_code": hotel_data.get("address", {}).get("postal_code", "NULL"),
                "full_address": f"{hotel_data.get("address", {}).get("line_1", "NULL")}, {hotel_data.get("address", {}).get("line_2", "NULL")}",
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": hotel_data.get("location", {}).get("coordinates", {}).get("latitude", "NULL"),
                    "longitude": hotel_data.get("location", {}).get("coordinates", {}).get("longitude", "NULL"),
                    "address_line_1": hotel_data.get("address", {}).get("line_1", "NULL"),
                    "address_line_2": hotel_data.get("address", {}).get("line_2", "NULL"),
                    "city": hotel_data.get("address", {}).get("city", "NULL"),
                    "state": hotel_data.get("address", {}).get("state_province_name", "NULL"),
                    "country": hotel_data.get("address", {}).get("country_code", "NULL"),
                    "country_code": hotel_data.get("address", {}).get("country_code", "NULL"),
                    "postal_code": hotel_data.get("address", {}).get("postal_code", "NULL"),
                    "full_address": f"{hotel_data.get("address", {}).get("line_1", "NULL")}, {hotel_data.get("address", {}).get("line_2", "NULL")}",
                    "google_map_site_link": google_map_site_link,
                    },
                "mapping": {
                    "continent_id": "NULL",
                    "country_id": hotel_data.get("address", {}).get("country_code", "NULL"),
                    "province_id": "NULL",
                    "state_id": "NULL",
                    "city_id": "NULL",
                    "area_id": "NULL"
                    }
                },

            "contacts": {
                "phone_numbers": [hotel_data.get("phone", "NULL")],
                "fax": hotel_data.get("fax", "NULL"),
                "email_address": hotel_data.get("email", "NULL"),
                "website": hotel_data.get("website", "NULL"),
                },
            
            "descriptions": descriptions_data,
            "room_type": structured_room_types,
            "spoken_languages": transformed_spoken_languages,
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
        






            
# content_vervotech = {}
# hotel_content = HotelContentEAN(content_vervotech=content_vervotech)


# hotel_id = "100001138"
# hotel_id = "8001703"
# hotel_details_one = hotel_content.hotel_details(hotel_id=hotel_id)
# content_data_json = json.dumps(hotel_details_one, indent=4) 
# print(content_data_json)
# print(hotel_details_one)



# hotel_id = "3009518"
# content_data = hotel_content.iit_hotel_content(hotel_id=hotel_id)
# content_data_json = json.dumps(content_data, indent=4) 
# print(content_data_json)
# print(type(content_data))



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


providerFamily = "EAN"
get_provider_ids = get_provider_hotel_id_list(engine=engine, table=table, providerFamily=providerFamily)

# print(get_provider_ids)



folder_name = "./HotelInfo/EAN"




for id in get_provider_ids:
    try:
        content_vervotech = {}
        hotel_content = HotelContentEAN(content_vervotech=content_vervotech)

        data = hotel_content.iit_hotel_content(hotel_id=id)

        if data is None:
            continue

        save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
        print(f"Completed Createing Json file for hotel: {id}")
    
    except ValueError:
        print(f"Skipping invalid id: {id}")
