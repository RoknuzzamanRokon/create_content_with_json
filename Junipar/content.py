import requests
import xmltodict
from sqlalchemy import create_engine, MetaData, Table, insert
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import sys
import pandas as pd

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


def get_hotel_id_list(table, engine):
    try:
        query = f"SELECT DISTINCT HotelId FROM {table}"
        df = pd.read_sql(query, engine)
        list = df['HotelId'].dropna().tolist()
        return list
    except Exception as e:
        print(f"Error {e}")

def read_tracking_file(file_path):
    """
    Reads tracking file and returns a list of cities yet to be processed.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file.readlines()]
    return []

def write_tracking_file(file_path, cities):
    """
    Writes the remaining cities to the tracking file.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        file.write("\n".join(cities))

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


def create_content_with_api(hotel_code):
        hotel_data = get_data_using_juniper_api(hotel_code=hotel_code)

        print(type(hotel_data))
        


        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())
        specific_data = {}

        hotel_content = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']

        specific_data['created'] = createdAt
        specific_data['timestamp'] = timeStamp
        specific_data['hotel_id'] = hotel_content['Zone']['@JPDCode']
        specific_data['name'] = hotel_content['HotelName']
        specific_data['name_local'] = hotel_content['HotelName']
        specific_data['hotel_formerly_name'] = hotel_content['HotelName']
        specific_data['destination_code'] = hotel_content['Zone']['@Code']
        specific_data['country_code'] = hotel_content['Address']['Address'].split(',')[-1].strip()
        specific_data['brand_text'] = hotel_content['HotelChain']['Name']
        specific_data['property_type'] = hotel_content['HotelCategory']['@Type']
        specific_data['star_rating'] = hotel_content['HotelCategory']['#text']
        specific_data['chain'] = "NULL"
        specific_data['brand'] = "NULL"
        specific_data['logo'] = "NULL"
        primary_image = hotel_content.get('Images', {}).get('Image', [])[0]
        specific_data['primary_photo'] = primary_image.get('@FileName', 'N/A')
                

        # specific_data['hotel_id'] = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']['Zone']['@JPDCode']

        # specific_data['name'] = hotel_data['soap:Envelope']['soap:Body']['HotelContentResponse']['ContentRS']['Contents']['HotelContent']['HotelName']

        address_line_1 = hotel_content.get("Address", {}).get("Address", None)

        hotel_name = hotel_content['HotelName']


        address_query = f"{address_line_1}, {hotel_name}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"


        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_content['Zone']['@JPDCode'],
            "name": hotel_content['HotelName'],
            "name_local": hotel_content['HotelName'],
            "hotel_formerly_name": hotel_content['HotelName'],
            "destination_code": hotel_content['Zone']['@Code'],
            "country_code":  hotel_content['Address']['Address'].split(',')[-1].strip(),
            "brand_text": None,
            "property_type": None,
            "star_rating": hotel_content['HotelCategory']['#text'],
            "chain": "NULL",
            "brand": "NULL",
            "logo": "NULL",
            "primary_photo": primary_image,
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
                "latitude": hotel_content.get("Address", {}).get("Latitude", None),
                "longitude": hotel_content.get("Address", {}).get("Longitude", None),
                "address_line_1": hotel_content.get("Address", {}).get("Address", None),
                "address_line_2": None,
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
                    "address_line_2": None,
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
                    "title": "Description",
                    "text": None
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
            "facilities": None,
            "hotel_photo": None, 
            
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
data = create_content_with_api(hotel_code='JP146952')


print(data)