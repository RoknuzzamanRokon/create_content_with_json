import requests
import hashlib
import time
from dotenv import load_dotenv
import os
import json
from datetime import datetime
import pandas as pd

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

            print(f'Get current time in seconds: {timestamp}')

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
                hotel_data = response_data.get(hotel_id, {})
                return hotel_data
            else:
                print(f"Error: Failed to restrieve data, status code: {response.status_code}")
                print(response.taxt)
                return {}
        
        except Exception as e:
            print(f"Error in hotel_details: {e}")

    





def iit_hotel_content(data):

    # df = pd.read_html()
    
    hotel_data = data


    createdAt = datetime.now()
    createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
    created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
    timeStamp = int(created_at_dt.timestamp())


    print("CreatedAt:", created_at_dt)
    print("Timestamp:", timeStamp)

    
    # # Construct the hotel photo data in the desired format
    # hotel_photo_data = [
    #     {
    #         "picture_id": "NULL",  
    #         "title": "NULL",       
    #         "url": url             
    #     } for url in hotel_info.get("imageUrls", []) or []
    # ]

    # hotel_room_amenities = [
    #     {
    #         "type": ameList,
    #         "title": ameList,
    #         "icon": "NULL"
    #     } for ameList in hotel_info.get("masterRoomAmenities", []) or []
    # ]
    
    # hotel_amenities = [
    #     {
    #         "type": ameList,
    #         "title": ameList,
    #         "icon": "NULL"
    #     } for ameList in hotel_info.get("masterHotelAmenities", []) or []
    # ]
    
    # address_line_1 = hotel_data.get("Address1", "NULL")
    # address_line_2 = hotel_data.get("Address2", "NULL")
    # hotel_name = hotel_info.get("name", hotel_data.get("HotelName", "NULL"))
    # # city = hotel_data.get("City", "NULL")
    # # postal_code = hotel_data.get("ZipCode", "NULL")
    # # state = hotel_info.get("address", {}).get("stateName", "NULL")
    # # country = hotel_data.get("CountryName", "NULL")

    # address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
    # google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"


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
        "primary_photo": hotel_data["images"][0].get("links", {}).get("1000px", {}).get("href"),
        # "review_rating": {
        #     "source": "NULL",
        #     "number_of_reviews": "NULL",
        #     "rating_average": hotel_info.get("tripAdvisorRating", "NULL"),
        #     "popularity_score": "NULL",
        # },
        # "policies": {
        #     "checkin": {
        #         "begin_time": "NULL",
        #         "end_time": "NULL",
        #         "instructions": "NULL",
        #         "special_instructions": "NULL",
        #         "min_age": "NULL",
        #     },
        #     "checkout": {
        #         "time": "NULL",
        #     },
        #     "fees": {
        #         "optional": "NULL",
        #     },
        #     "know_before_you_go": "NULL",
        #     "pets": "NULL",
        #     "remark": "NULL",
        #     "child_and_extra_bed_policy": {
        #         "infant_age": "NULL",
        #         "children_age_from": "NULL",
        #         "children_age_to": "NULL",
        #         "children_stay_free": "NULL",
        #         "min_guest_age": "NULL"
        #     },
        #     "nationality_restrictions": "NULL",
        # },
        # "address": {
        #     "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", "NULL")),
        #     "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", "NULL")),
        #     "address_line_1": hotel_data.get("Address1", "NULL"),
        #     "address_line_2": hotel_data.get("Address2", "NULL"),
        #     "city": hotel_data.get("City", "NULL"),
        #     "state": hotel_info.get("address", {}).get("stateName", "NULL"),
        #     "country": hotel_data.get("CountryName", "NULL"),
        #     "country_code": hotel_data.get("CountryCode", "NULL"),
        #     "postal_code": hotel_data.get("ZipCode", "NULL"),
        #     "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}",
        #     "google_map_site_link": google_map_site_link,
        #     "local_lang": {
        #         "latitude": hotel_info.get("geocode", {}).get("lat", hotel_data.get("Latitude", "NULL")),
        #         "longitude": hotel_info.get("geocode", {}).get("lon", hotel_data.get("Longitude", "NULL")),
        #         "address_line_1": hotel_data.get("Address1", "NULL"),
        #         "address_line_2": hotel_data.get("Address2", "NULL"),
        #         "city": hotel_data.get("City", "NULL"),
        #         "state": hotel_info.get("address", {}).get("stateName", "NULL"),
        #         "country": hotel_data.get("CountryName", "NULL"),
        #         "country_code": hotel_data.get("CountryCode", "NULL"),
        #         "postal_code": hotel_data.get("ZipCode", "NULL"),
        #         "full_address": f"{hotel_data.get('Address1', 'NULL')}, {hotel_data.get('Address2', 'NULL')}", 
        #         "google_map_site_link": google_map_site_link,
        #     },
        #     "mapping": {
        #         "continent_id": "NULL",
        #         "country_id": hotel_data.get("CountryCode", "NULL"),
        #         "province_id": "NULL",
        #         "state_id": "NULL",
        #         "city_id": "NULL",
        #         "area_id": "NULL"
        #     }
        # },
        # "contacts": {
        #     "phone_numbers": [hotel_info.get("contact", {}).get("phoneNo", "NULL")],
        #     "fax": hotel_info.get("contact", {}).get("faxNo", "NULL"),
        #     "email_address": "NULL",
        #     "website": hotel_info.get("contact", {}).get("website", hotel_data.get("Website", "NULL"))
        # },
        # "descriptions": [
        #     {
        #         "title": "NULL",
        #         "text": "NULL"
        #     }
        # ],
        # "room_type": {
        #     "room_id": "NULL",
        #     "title": "NULL",
        #     "title_lang": "NULL",
        #     "room_pic": "NULL",
        #     "description": "NULL",
        #     "max_allowed": {
        #     "total": "NULL",
        #     "adults": "NULL",
        #     "children": "NULL",
        #     "infant": "n/a"
        #     },
        #     "no_of_room": "n/a",
        #     "room_size": "NULL",
        #     "bed_type": [
        #             {
        #             "description": "NULL",
        #             "configuration": [
        #                 {
        #                 "quantity": "NULL",
        #                 "size": "NULL",
        #                 "type": "NULL"
        #                 }
        #             ],
        #             "max_extrabeds": "n/a"
        #             }
        #         ],
        #     "shared_bathroom": "n/a"
        #     },
        # "spoken_languages": {
        #     "type": "spoken_languages",
        #     "title": "English",
        #     "icon": "mdi mdi-translate-variant"
        #     },
        # "amenities": hotel_room_amenities,
        # "facilities": hotel_amenities,
        # "hotel_photo": hotel_photo_data, 
        
        # "point_of_interests": [
        #     {
        #     "code": "NULL",
        #     "name": "NULL"
        #     }
        # ],
        # "nearest_airports": [
        #     {
        #     "code": "NULL",
        #     "name": "NULL"
        #     }
        # ],
        # "train_stations": [
        #     {
        #     "code": "NULL",
        #     "name": "NULL"
        #     }
        # ], 
        # "connected_locations": [
        #     {
        #     "code": "NULL",
        #     "name": "NULL"
        #     },
        # ],
        # "stadiums": [
        #     {
        #     "code": "NULL",
        #     "name": "NULL"
        #     }
        # ]
    }


    return specific_data






            
content_vervotech = {}

hotel_content = HotelContentEAN(content_vervotech=content_vervotech)

hotel_id = "100001138"
hotel_details = hotel_content.hotel_details(hotel_id=hotel_id)





name = hotel_details.get("name")
print(name)
# print(hotel_details)
# print(type(hotel_details))


content_data = iit_hotel_content(data=hotel_details)
print(content_data)