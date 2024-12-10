from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import xmltodict
import requests
import random
import json
import os
import re


load_dotenv()

# Database connection setup
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL_SERVER)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
vervotech_table = Table("vervotech_mapping", metadata, autoload_with=engine)


class Hotel:
    def __init__(self, provider_hotel_id, provider_family, hotel_city, hotel_country, country_code, hotel_longitude, hotel_latitude):
        self.ProviderHotelId = provider_hotel_id
        self.ProviderFamily = provider_family
        self.hotel_city = hotel_city
        self.hotel_country = hotel_country
        self.hotel_country_code = country_code
        self.hotel_longitude = hotel_longitude
        self.hotel_latitude = hotel_latitude

    def __repr__(self):
        return f"""Hotel(ProviderHotelId={self.ProviderHotelId},
                ProviderFamily={self.ProviderFamily},
                hotel_city={self.hotel_city},
                hotel_country={self.hotel_country},
                hotel_longitude={self.hotel_longitude},
                hotel_latitude={self.hotel_latitude})"""





def search_hotel_by_id(file_name, hotel_id):
    with open(file_name, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            hotel_data = eval(line)
            if hotel_data[0] == hotel_id:
                return Hotel(*hotel_data)
    return None


def get_data_from_vervotech_mapping_table(engine):
    query = "SELECT ProviderHotelId, ProviderFamily, hotel_city, hotel_country, country_code, hotel_longitude, hotel_latitude FROM vervotech_mapping WHERE ProviderFamily = 'GoGlobal'"
    df = pd.read_sql(query, engine)
    result_list = df.to_records(index=False).tolist()
    return result_list

def save_data_to_file(data, file_name):
    data_as_string = "\n".join([str(item) for item in data])
    file_path = os.path.join(os.getcwd(), file_name)
    
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(data_as_string)
    
    print(f"Data successfully saved to {file_path}")


def get_data_from_goglobal_api(hotel_id):
    agency = os.getenv('GOGLOBAL_AGENCY')
    user = os.getenv('GOGLOBAL_USER')
    password = os.getenv('GOGLOBAL_PASSWORD')
    url = "https://gtr.xml.goglobal.travel/xmlwebservice.asmx"

    payload = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                        xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                        xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
                <soap12:Body>
                    <MakeRequest xmlns="http://www.goglobal.travel/">
                            <requestType>6</requestType>
                            <xmlRequest>
                                <![CDATA[
                                    <Root>
                                        <Header>
                                            <Agency>{agency}</Agency>
                                            <User>{user}</User>
                                            <Password>{password}</Password>
                                            <Operation>HOTEL_INFO_REQUEST</Operation>
                                            <OperationType>Request</OperationType>
                                        </Header>
                                        <Main Version="2.2">
                                            <InfoHotelId>{hotel_id}</InfoHotelId>
                                            <InfoLanguage>en</InfoLanguage>
                                        </Main>
                                    </Root>
                                ]]>
                            </xmlRequest>
                    </MakeRequest>
                </soap12:Body>
            </soap12:Envelope>"""


    headers = {
            'Content-Type': 'application/soap+xml; charset=utf-8',
            'API-Operation': 'HOTEL_INFO_REQUEST'
            }


    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status() 
        parsed_response = xmltodict.parse(response.text)
        return parsed_response       
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An error occurred during parsing: {e}")




def updata_data_in_innova_table(hotel_id):
    try:
        root_data = get_data_from_goglobal_api(hotel_id)

        if not root_data:
            print(f"No data found for hotel ID {hotel_id}")
            return None
        xml_data = root_data["soap:Envelope"]["soap:Body"]["MakeRequestResponse"]["MakeRequestResult"]

        hotel_data = ET.fromstring(xml_data)

        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())


        # Address handling
        address_line_1_node = hotel_data.find(".//Address")
        address_line_1 = address_line_1_node.text if address_line_1_node is not None else None


        # Construct full address
        full_address = ', '.join(filter(None, [address_line_1]))
        google_map_site_link = (
            f"http://maps.google.com/maps?q={full_address.replace(' ', '+')}"
            if full_address
            else None
        )


        # Colocet data from txt document in local file.
        file_name = "collect_some_goglobal_data.txt"

        if not os.path.exists(file_name):
            data = get_data_from_vervotech_mapping_table(engine)
            save_data_to_file(data, file_name)

        result = search_hotel_by_id(file_name, hotel_id)

        # print(result.ProviderFamily)


        # Picture area section here.
        pictures = hotel_data.find(".//Pictures")
        photo_data = []

        if pictures is not None:
            for picture in pictures.findall("Picture"):
                url = picture.text.strip() if picture.text else ""
                
                # Extract the Description attribute as title
                title = picture.attrib.get("Description", "")
                photo_data.append({
                    "picture_id": None,
                    "title": title,
                    "url": url
                })

        primary_photo_url = None  

        for photo in photo_data:
            if photo['title'] == 'Primary image':
                primary_photo_url = photo['url']
                break


        # Process each facility into the desired JSON structure
        hotel_hotelFacilities = hotel_data.find(".//HotelFacilities").text
        if hotel_hotelFacilities is None:
            facilities_json = None
        else:
            facilities_list = hotel_hotelFacilities.split("<BR />")
            
            facilities_json = []
            for facility in facilities_list:
                facility = facility.strip()  
                if facility:  
                    facilities_json.append({
                        "type": facility,
                        "title": facility,
                        "icon": None
                    })
            # print(f"hotel name = {facilities_json}")


        # Proceess each room facility into content format.
        hotel_roomFacilities = hotel_data.find(".//RoomFacilities").text
        if hotel_roomFacilities is None:
            rm_facilities_json = None
        else:
            roomFacilities_list = hotel_roomFacilities.split("<BR />")

            rm_facilities_json = []
            for facility in roomFacilities_list:
                facility = facility.strip()
                if facility:
                    rm_facilities_json.append({
                        "type": facility,
                        "title": facility,
                        "icon": None
                    })
            # print(rm_facilities_json)


        # Main Description area here.
        hotel_description_node = hotel_data.find(".//Description")
        hotel_description = hotel_description_node.text if hotel_description_node is not None else None

        if hotel_description:
            checkin_time_match = re.search(r"Checkin Time: (\d{2}:\d{2})", hotel_description)
            checkout_time_match = re.search(r"Checkout End Time: (\d{2}:\d{2})", hotel_description)
            pets_match = re.search(r"Pets are allowed.*?(\. No extra charges\.?)", hotel_description)

            description_text = re.split(r"<BR />|<b>", hotel_description)[0].strip() if "<BR />" in hotel_description or "<b>" in hotel_description else hotel_description.strip()
        else:
            checkin_time_match = None
            checkout_time_match = None
            pets_match = None
            description_text = None

        checkin_time = checkin_time_match.group(1) if checkin_time_match else None
        checkout_time = checkout_time_match.group(1) if checkout_time_match else None
        pets_info = pets_match.group(0) if pets_match else None

        # Fax and phone number details.
        phone_node = hotel_data.find(".//Phone")
        phone_numbers = phone_node.text if phone_node is not None else None

        fax_node = hotel_data.find(".//Fax")
        fax = fax_node.text if fax_node is not None else None

        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_data.find(".//HotelId").text or None,
            "name": hotel_data.find(".//HotelName").text  or None,
            "name_local": hotel_data.find(".//HotelName").text  or None,
            "hotel_formerly_name": hotel_data.find(".//HotelName").text  or None,
            "destination_code": None,
            "country_code":  result.hotel_country_code or None,
            "brand_text": None,
            "property_type": None,
            "star_rating": None,
            "chain": None,
            "brand": None,
            "logo": None,
            "primary_photo": primary_photo_url,
            "review_rating": {
                "source": None,
                "number_of_reviews": None,
                "rating_average": None,
                "popularity_score": None
            },
            "policies": {
                "checkin": {
                    "begin_time": checkin_time,
                    "end_time": checkout_time,
                    "instructions": None,
                    "special_instructions": None,
                    "min_age": None,
                },
                "checkout": {
                    "time": checkout_time,
                },
                "fees": {
                    "optional": None,
                },
                "know_before_you_go": None,
                "pets": pets_info,
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
                "latitude": result.hotel_latitude or None,
                "longitude": result.hotel_longitude or None,
                "address_line_1": address_line_1 or None,
                "address_line_2": None,
                "city": result.hotel_city,
                "state": None,
                "country": result.hotel_country or None,
                "country_code": result.hotel_country_code or None,
                "postal_code": None,
                "full_address": full_address,
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": result.hotel_latitude or None,
                    "longitude": result.hotel_longitude or None,
                    "address_line_1": address_line_1 or None, 
                    "address_line_2": None,
                    "city": result.hotel_city or None,
                    "state": None,
                    "country": result.hotel_country or None,
                    "country_code": result.hotel_country_code or None,
                    "postal_code": None,
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
                "phone_numbers": phone_numbers,
                "fax": fax,
                "email_address": None,
                "website": None,
            },
            "descriptions": [
                {
                    "title": "Description",
                    "text": description_text
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
            "amenities": rm_facilities_json,
            "facilities": facilities_json,
            "hotel_photo": photo_data,
            
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



def get_hotel_id_list(engine, table):
    """
    Here get all hotel id list.
    """
    with engine.connect() as conn:
        query = select(table.c.ProviderHotelId).where(table.c.ProviderFamily =='GoGlobal')
        
        df = pd.read_sql(query, conn)
        
        hotel_id_list = df['ProviderHotelId'].tolist()
        
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




folder_path = '../HotelInfo/GoGlobal'
tracking_file_path = 'tracking_file_for_goglobal_content_create.txt'


save_json_files_follow_systemId(folder_path, tracking_file_path, vervotech_table, engine)


