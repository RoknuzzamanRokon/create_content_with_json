import requests
import xmltodict
from sqlalchemy import create_engine, MetaData, Table, insert
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import sys
import pandas as pd
import json

# Load environment variables
load_dotenv()

juniper_pass = os.getenv("JUNIPER_PASS")
juniper_mail = os.getenv("JUNIPER_EMAIL")


logging.basicConfig(
    filename="content_create_juniper_hotel_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)



DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

juniper_table = metadata.tables.get('juniper')



def get_payload(password, email, hotel_code):
    payload = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://www.juniper.es/webservice/2007/">
    <soapenv:Header/>
    <soapenv:Body>
        <HotelContent>
            <HotelContentRQ Version="1" Language="en">
                <Login Password="{password}" Email="{email}"/>
                <HotelContentList>
                    <Hotel Code="{hotel_code}"/>
                </HotelContentList>
            </HotelContentRQ>
        </HotelContent>
    </soapenv:Body>
</soapenv:Envelope>"""
    return payload



def get_data_using_juniper_api(hotel_code):
    url = "https://xml-uat.bookingengine.es/WebService/jp/operations/staticdatatransactions.asmx"


    juniper_pass = os.getenv("JUNIPER_PASS")
    juniper_mail = os.getenv("JUNIPER_EMAIL")

    payload = get_payload(juniper_pass, juniper_mail, hotel_code)
    headers = {
    'Content-Type': 'text/xml;charset=UTF-8',
    'SOAPAction': '"http://www.juniper.es/webservice/2007/HotelContent"',
    'Cookie': 'StatC=8l+HAPXk1NeIsJR8qq/ISw==; StatP=UAvxTZg5/KSb5xjepzejGq+uRWuHJ5ucWURdLMNyct+5jbHBZLi9nj/RVgL8/LVZ; idioma=en'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    # print(response.text)
    data_dict = xmltodict.parse(response.text)
    return data_dict








def process_room_data(hotel_rooms):
    if not hotel_rooms:
        print("No room data available. Skipping room processing.")
        return []  

    room_data_list = []

    for idx, room in enumerate(hotel_rooms, start=1):  
        description = room.get("Description", "")
        size_room = None
        bed_type = []
        
        if "size_room:" in description:
            size_room = description.split("size_room:")[-1].strip()
        
        if "(" in description and ")" in description:
            bed_info = description.split("(")[-1].split(")")[0].strip()
            bed_type.append({
                "description": bed_info,
                "configuration": [
                    {
                        "quantity": "1",  
                        "size": "double",  
                        "type": bed_info,
                    }
                ],
                "max_extrabeds": ""
            })
        
        # Handle RoomOccupancy safely
        room_occupancy = room.get("RoomOccupancy", {})
        max_adults = int(room_occupancy.get("@MaxAdults", 0))
        max_children = int(room_occupancy.get("@MaxChildren", 0))

        room_data = {
            "room_type": {
                "room_id": f"Room_{idx}",
                "title": room.get("Name", ""),
                "title_lang": "",  
                "room_pic": "", 
                "description": description,
                "max_allowed": {
                    "total": max_adults + max_children,
                    "adults": max_adults,
                    "children": max_children,
                    "infant": 0  
                },
                "no_of_room": "",  
                "room_size": size_room,
                "bed_type": bed_type,
                "shared_bathroom": ""  
            }
        }
        room_data_list.append(room_data)
    
    return room_data_list






def create_content_with_api(hotel_code):
        hotel_data = get_data_using_juniper_api(hotel_code=hotel_code)

        # Skip the process if hotel_data is None
        if hotel_data is None:
            print(f"Error: No data returned for hotel code {hotel_code}. Skipping.")
            return None  # Or continue the loop if you're calling this function in a loop

        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())
        specific_data = {}

        try:
            hotel_content = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']
        except (TypeError, KeyError, AttributeError) as e:
            print(f"Error accessing hotel data for hotel code {hotel_code}: {e}")
            return None  # Skip this hotel and move to the next one

        # Get primary image here.
        primary_image = None
        primary_file_name = None
        try:
            images = hotel_content.get('Images', {}).get('Image', [])
            if isinstance(images, list) and len(images) > 0:
                primary_image = images[0]
            else:
                primary_image = images  
            primary_file_name = primary_image.get("FileName") if primary_image else None
        except (IndexError, AttributeError):
            print("Primary image not found or invalid. Skipping.")

        # Get description text and title here.
        description_title = None
        description_text = None
        try:
            descriptions = hotel_content.get("Descriptions", {}).get("Description", [])
            if isinstance(descriptions, list):
                for description in descriptions:
                    desc_type = description.get("@Type", None)
                    if desc_type == "LNG":
                        description_title = description.get("#text", None)
                    elif desc_type == "ROO":
                        description_text = description.get("#text", None)
            else:
                desc_type = descriptions.get("@Type", None)
                if desc_type == "LNG":
                    description_title = descriptions.get("#text", None)
                elif desc_type == "ROO":
                    description_text = descriptions.get("#text", None)
        except (KeyError, AttributeError):
            print("Descriptions key is missing or invalid. Skipping.")

        address_line_1 = hotel_content.get("Address", {}).get("Address", None)
        hotel_name = hotel_content['HotelName']
        address_query = f"{address_line_1}, {hotel_name}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"

        # Facility entries
        facility_entries = []
        try:
            features = hotel_content["Features"]["Feature"]
            if not isinstance(features, list):
                features = [features]
            for feature in features:
                facility_entry = {
                    "type": feature.get("@Type", None),
                    "title": feature.get("#text", None),
                    "icon": "mdi mdi-alpha-f-circle-outline"
                }
                facility_entries.append(facility_entry)
        except KeyError:
            print("Features key is missing. Skipping this part.")
                    
        # Hotel image photo
        images = hotel_content.get("Images", {}).get("Image", [])
        if not isinstance(images, list):
            images = [images] if images else []  

        image_entries = []
        for image in images:
            if image: 
                entry = {
                    "picture_id": None,  
                    "title": image.get("Title", None),  
                    "url": image.get("FileName", None)  
                }
                image_entries.append(entry)

        if not image_entries:
            print("No images found for this hotel.")

        # Check time here.
        check_time = hotel_content.get("TimeInformation", {}).get("CheckTime", {}) or None
        if check_time:
            checkin = check_time.get("@CheckIn", None)
            checkout = check_time.get("@CheckOut", None)
        else:
            checkin = None
            checkout = None

        hotel_rooms = hotel_content.get("HotelRooms", {}).get("HotelRoom", {}) or None
        if isinstance(hotel_rooms, dict): 
            hotel_rooms = [hotel_rooms]

        processed_data = process_room_data(hotel_rooms)

        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_content['Zone']['@JPDCode'],
            "name": hotel_content['HotelName'],
            "name_local": hotel_content['HotelName'],
            "hotel_formerly_name": hotel_content['HotelName'],
            "destination_code": None,
            "country_code":  hotel_content['Address']['Address'].split(',')[-1].strip(),
            "brand_text": None,
            "property_type": None,
            "star_rating": None,
            "chain": "NULL",
            "brand": "NULL",
            "logo": "NULL",
            "primary_photo": primary_file_name,
            "review_rating": {
                "source": None,
                "number_of_reviews": None,
                "rating_average": None,
                "popularity_score": None
            },
            "policies": {
                "checkin": {
                    "begin_time": checkin,
                    "end_time": checkout,
                    "instructions": None,
                    "special_instructions": None,
                    "min_age": None,
                },
                "checkout": {
                    "time": checkin,
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
                "latitude": hotel_content.get("Address", {}).get("Latitude", None),
                "longitude": hotel_content.get("Address", {}).get("Longitude", None),
                "address_line_1": hotel_content.get("Address", {}).get("Address", None),
                "address_line_2": hotel_content['Address']['Address'].split(',')[-1].strip(),
                "city": None,
                "state": None,
                "country": hotel_content['Address']['Address'].split(',')[-1].strip(),
                "country_code": hotel_content['Address']['Address'].split(',')[-1].strip(),
                "postal_code": hotel_content.get("Address", {}).get("PostalCode", None),
                "full_address": f"{hotel_content.get("Address", {}).get("Address", None)}",
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": hotel_content.get("Address", {}).get("Latitude", None),
                    "longitude": hotel_content.get("Address", {}).get("Longitude", None),
                    "address_line_1": hotel_content.get("Address", {}).get("Address", None),
                    "address_line_2": hotel_content['Address']['Address'].split(',')[-1].strip(),
                    "city": None,
                    "state": None,
                    "country": hotel_content['Address']['Address'].split(',')[-1].strip(),
                    "country_code": hotel_content['Address']['Address'].split(',')[-1].strip(),
                    "postal_code": hotel_content.get("Address", {}).get("PostalCode", None),
                    "full_address": f"{hotel_content.get("Address", {}).get("Address", None)}",
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
                "phone_numbers": None,
                "fax": None,
                "email_address": None,
                "website": None,
            },
            "descriptions": [
                {
                    "title": description_title,
                    "text": description_text
                }
            ],
            "room_type": processed_data,
            "spoken_languages": {
                "type": None,
                "title": None,
                "icon": None
                },
            "amenities": None,
            "facilities": facility_entries,
            "hotel_photo": image_entries, 
            
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
        
        hotel_category = hotel_content.get('HotelCategory', None)
        if isinstance(hotel_category, dict):
            specific_data["star_rating"] = hotel_category.get('#text', None)
        elif isinstance(hotel_category, str):
            specific_data["star_rating"] = hotel_category
        else:
            specific_data["star_rating"] = None

        return specific_data


def save_json_to_folder(data, hotel_id, folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    file_path = os.path.join(folder_name, f"{hotel_id}.json")
    try:
        with open(file_path, "w") as json_file:
            json.dump(data, json_file, indent=4)
        logger.info(f"Data saved to {file_path}")
    except TypeError as e:
        logger.error(f"Serialization error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def get_hotel_id_list(table, engine):
    try:
        query = f"SELECT DISTINCT HotelId FROM {table}"
        df = pd.read_sql(query, engine)
        list = df['HotelId'].dropna().tolist()
        return list
    except Exception as e:
        logger.error(f"Error {e}")
        


get_provider_ids = get_hotel_id_list(table=juniper_table, engine=engine)

folder_name = "../HotelInfo/Juniper"

for id in get_provider_ids:
    try:
        file_path = os.path.join(folder_name, f"{id}.json")
        
        if os.path.exists(file_path):
            logger.info(f"Hotel {id} already exists. Skipping.")
            continue 

        # Fetch hotel data
        data = create_content_with_api(hotel_code=id)
        
        if data is None:
            continue  

        save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
        logger.info(f"Completed Creating JSON file for hotel: {id}")
    
    except ValueError:
        logger.error(f"Skipping invalid id: {id}")
    except Exception as e:
        logger.error(f"Error processing hotel {id}: {e}")