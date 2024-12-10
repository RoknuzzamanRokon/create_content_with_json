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
    filename="juniper_list_data_log.log",
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
    room_data_list = []
    
    for idx, room in enumerate(hotel_rooms, start=1):  # Enumerate for room_id
        # Parse room size and bed type from description
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
                        "quantity": "n/a",  
                        "size": "n/a",  
                        "type": bed_info,
                    }
                ],
                "max_extrabeds": ""
            })
        
        # Build the room data dictionary
        room_data = {
            "room_type": {
                "room_id": f"Room_{idx}",
                "title": room.get("Name", ""),
                "title_lang": "",  
                "room_pic": "", 
                "description": description,
                "max_allowed": {
                    "total": int(room["RoomOccupancy"]["@MaxAdults"]) + int(room["RoomOccupancy"]["@MaxChildren"]),
                    "adults": int(room["RoomOccupancy"]["@MaxAdults"]),
                    "children": int(room["RoomOccupancy"]["@MaxChildren"]),
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

        print(type(hotel_data))
        


        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())
        specific_data = {}

        hotel_content = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']

        # specific_data['created'] = createdAt
        # specific_data['timestamp'] = timeStamp
        # specific_data['hotel_id'] = hotel_content['Zone']['@JPDCode']
        # specific_data['name'] = hotel_content['HotelName']
        # specific_data['name_local'] = hotel_content['HotelName']
        # specific_data['hotel_formerly_name'] = hotel_content['HotelName']
        # specific_data['destination_code'] = hotel_content['Zone']['@Code']
        # specific_data['country_code'] = hotel_content['Address']['Address'].split(',')[-1].strip()
        # specific_data['brand_text'] = hotel_content['HotelChain']['Name']
        # specific_data['property_type'] = hotel_content['HotelCategory']['@Type']
        # specific_data['star_rating'] = hotel_content['HotelCategory']['#text']
        # specific_data['chain'] = None
        # specific_data['brand'] = None
        # specific_data['logo'] = None
        # primary_image = hotel_content.get('Images', {}).get('Image', [])[0]
        # specific_data['primary_photo'] = primary_image.get('@FileName', 'N/A')
                

        # Get primary image here.
        primary_image = hotel_content.get('Images', {}).get('Image', [])[0]
        primary_file_name = primary_image.get("FileName") if primary_image else None

        # Get description text and title here.
        descriptions = hotel_content["Descriptions"]["Description"]

        description_title = None
        description_text = None

        if isinstance(descriptions, list):
            for description in descriptions:
                desc_type = description["@Type"]
                if desc_type == "LNG":
                    description_title = description["#text"]
                elif desc_type == "ROO":
                    description_text = description["#text"]
        else:
            desc_type = description["@Type"]
            if desc_type == "LNG":
                description_title = description["#text"]
            elif desc_type == "ROO":
                description_text = description["#text"]


        # specific_data['hotel_id'] = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']['Zone']['@JPDCode']

        # specific_data['name'] = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']['HotelName']
        address_line_1 = hotel_content.get("Address", {}).get("Address", None)
        hotel_name = hotel_content['HotelName']
        address_query = f"{address_line_1}, {hotel_name}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None


        # Facilitys area here.
        features = hotel_content["Features"]["Feature"]
        if not isinstance(features, list):
            features = [features]

        facility_entries = []
        for feature in features:
            facility_entry = {
                "type": feature["@Type"],
                "title": feature["#text"],
                "icon": "mdi mdi-alpha-f-circle-outline"
            }
            facility_entries.append(facility_entry)
            

        # Hotel image phote.
        images = hotel_content["Images"]["Image"]
        if not isinstance(images, list):
            images = [images]

        # Process the image data
        image_entries = []
        for image in images:
            entry = {
                "picture_id": None,
                "title": image["Title"],
                "url": image["FileName"]
            }
            image_entries.append(entry)


        # Check time here.
        check_time = hotel_content.get("TimeInformation", {}).get("CheckTime", {}) or None

        if check_time:
            checkin = check_time.get("@CheckIn", None)
            checkout = check_time.get("@CheckOut", None)
        else:
            checkin = None
            checkout = None

        # check_time = hotel_content["TimeInformation"]["CheckTime"]
        # checkin = check_time["@CheckIn"]
        # checkout = check_time["@CheckOut"]

        hotel_rooms = hotel_content.get("HotelRooms", {}).get("HotelRoom", {}) or None
        # hotel_rooms = hotel_content["HotelRooms"]["HotelRoom"]
        if isinstance(hotel_rooms, dict): 
            hotel_rooms = [hotel_rooms]

        processed_data = process_room_data(hotel_rooms)


        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_content['@Code'],
            "name": hotel_content['HotelName'],
            "name_local": hotel_content['HotelName'],
            "hotel_formerly_name": hotel_content['HotelName'],
            "destination_code": None,
            "country_code":  hotel_content['Address']['Address'].split(',')[-1].strip(),
            "brand_text": None,
            "property_type": None,
            "star_rating": hotel_content.get('HotelCategory', {}).get('#text', None),
            "chain": None,
            "brand": None,
            "logo": None,
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
                "nationality_restrictions": None,
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

        return specific_data


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


def get_hotel_id_list(table, engine):
    try:
        query = f"SELECT DISTINCT HotelId FROM {table}"
        df = pd.read_sql(query, engine)
        list = df['HotelId'].dropna().tolist()
        return list
    except Exception as e:
        print(f"Error {e}")
        


get_provider_ids = get_hotel_id_list(table=juniper_table, engine=engine)


folder_name = "./HotelInfo/juniper"



for id in get_provider_ids:
    try:
        # print(id)
        data = create_content_with_api(hotel_code=id)
        if data is None:
            continue

        save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
        print(f"Completed Createing Json file for hotel: {id}")
    
    except ValueError:
        print(f"Skipping invalid id: {id}")



# data = create_content_with_api(hotel_code='JP146952')


# print(data)