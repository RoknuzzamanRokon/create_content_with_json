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
import random

load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)


table='vervotech_mapping'

class HotelContentTBO:
    def __init__(self, content_expedia):
        load_dotenv()

        self.credentials = {
            'user_name': os.getenv('TOB_USERNAME'),
            'user_password': os.getenv('TOB_PASSWORD'),
            'base_url': os.getenv('TOB_BASE_URL')
        }
        self.expedia = content_expedia


    def hotel_api_authentication(self):
        try:
            user_name = self.credentials['user_name'].strip()
            user_password = self.credentials['user_password'].strip()
            base_url = self.credentials['base_url'].strip()

            # timestamp = int(time.time())
            authorization_basic = base64.b64encode(f"{user_name}:{user_password}".encode()).decode()
            
            data = {
                'username': user_name,
                'password': user_password,
                'base_url': base_url,
                'authorization_basic': authorization_basic
            }
            return data
        except Exception as e:
            print(f"Error in hotel api authentication: {e}")

    
    def hotel_details(self, hotel_id):
        try:
            hotel_id = hotel_id.strip()
            credentials_data = self.hotel_api_authentication()
            authorization_basic = credentials_data['authorization_basic']
            base_url = credentials_data['base_url']

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Basic {authorization_basic}"
            }

            payload = {
                "Hotelcodes": hotel_id,
                "Language": "en"
            }

            response = requests.post(f"{base_url}/Hoteldetails", headers=headers, json=payload)

            if response.status_code == 200:
                supplier_response_data = response.json()
                hotel_data = supplier_response_data.get('HotelDetails', [None])[0]
                return hotel_data
            else:
                print(f"Failed to fetch hotel details: {response.text}")
                return None
        except Exception as e:
            print(f"Error in fetching hotel details: {e}")
    

    def get_content_by_provider_hotel_ids(self, hotel_id):
        try:
            payload = json.dumps({
                "ProviderHotelIdentifiers": [
                    {
                    "ProviderHotelId": f"{hotel_id}",
                    "ProviderFamily": "TBO"
                    }
                ]
                })
            headers = {
                'accountid': 'gtrs',
                'apikey': os.getenv("VERVOTECH_API_KEY"),
                'Content-Type': 'application/json'
                }
            
            url = "https://hotelmapping.vervotech.com/api/3.0/content/GetCuratedContentByProvider"

            response = requests.request("POST", url=url, headers=headers, data=payload, timeout=10)

            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch curated content: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"Error in fetching hotel details: {e}")



    def iit_hotel_content(self, hotel_id):

        # df = pd.read_html()
        
        hotel_data = self.hotel_details(hotel_id)

        curated_hotels = self.get_content_by_provider_hotel_ids(hotel_id)
        vervotech_hotel_data = curated_hotels['CuratedHotels'][0]

        # print(vervotech_hotel_data)

        # data = vervotech_hotel_data.get("Latitude", None),
        # print(data)



        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())
        



        # Genarate data for google links.
        address_line_1 = vervotech_hotel_data.get("AddressLine1", None)
        address_line_2 = vervotech_hotel_data.get("AddressLine2", None)
        hotel_name = hotel_data.get("HotelName", None)
       

        address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
        google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None




        # Get email value in vervotech hotel.
        formatted_email = None  
        curated_hotels = vervotech_hotel_data.get("CuratedHotels", [])

        if curated_hotels:  
            email_list = curated_hotels[0].get("Emails", [])
            
            if email_list:
                formatted_email = [value for value in email_list] 




        # Get facilities in vervotech hotel.
        facilities_list = vervotech_hotel_data.get("Facilities", [])
        formatted_facilities = []

        for value in facilities_list:
            facility_entry = {
                "type": value.get("Name", None),
                "title": value.get("Name", None),
                "icon": "mdi mdi-alpha-f-circle-outline"
            }
            formatted_facilities.append(facility_entry)


        # Get airport in vervotech hotel.
        nearest_airports = vervotech_hotel_data.get("Airports", [])
        formatted_airports = []

        if not nearest_airports:
            formatted_airports = None
        else:
            for value in nearest_airports:
                airports_entry = {
                    "code": value.get("Code", None),
                    "name": value.get("Name", None)
                }
                formatted_airports.append(airports_entry)


        # Get airport in vervotech hotel.
        train_stations = vervotech_hotel_data.get("TrainStations", [])
        formatted_train_stations = []

        if not train_stations:
            formatted_train_stations = None
        else:
            for value in train_stations:
                train_stations_entry = {
                    "code": value.get("Code", None),
                    "Name": value.get("Name", None)
                }
                formatted_train_stations.append(train_stations_entry)
        

        # Get pointOfInterests in vervotech hotel.
        pointOfInterests = vervotech_hotel_data.get("PointOfInterests", [])
        formatted_pointOfInterests = []

        if not pointOfInterests:
            formatted_pointOfInterests = None
        else:
            for value in pointOfInterests:
                pointOfInterests_entry = {
                    "code": value.get("Code", None),
                    "Name": value.get("Name", None)
                }
                formatted_pointOfInterests.append(pointOfInterests_entry)
            

        # Get connected_locations in vervotech hotel.
        connected_locations = vervotech_hotel_data.get("ConnectedLocations", [])
        formatted_connected_locations = []

        if not connected_locations:
            formatted_connected_locations = None
        else:
            for value in connected_locations:
                connected_locations_entry = {
                    "code": value.get("Code", None),
                    "Name": value.get("Name", None)
                }
                formatted_connected_locations.append(connected_locations_entry)
        

        # Get stadiums in vervotech hotel.
        stadiums = vervotech_hotel_data.get("Stadiums", [])
        formatted_stadiums = []

        if not stadiums:
            formatted_stadiums = None
        else:
            for value in stadiums:
                stadiums_entry = {
                    "code": value.get("Code", None),
                    "Name": value.get("Name", None)
                }
                formatted_stadiums.append(stadiums_entry)



        # Extract the images from the hotel data
        hotel_data = self.hotel_details(hotel_id)
        images = hotel_data.get("Images", [])
        hotel_photos_all = []

        if not images:
             hotel_photos_all = None
        else:
            for image_url in images:
                photo_entry = {
                    "picture_id": None,
                    "title": None,
                    "url": image_url
                }
                hotel_photos_all.append(photo_entry)

        
        # Get Discriptions in supplier data.
        hotel_data = self.hotel_details(hotel_id)

        if not hotel_data:
            description_info = None
        else:
            description_info = {
                "title": "Description",
                "text": hotel_data.get("Description", "No description available")
            }

        
        specific_data = {
            "created": createdAt_str,
            "timestamp": timeStamp,
            "hotel_id": hotel_data.get("HotelCode", None),
            "name": hotel_data.get("HotelName", None),
            "name_local": hotel_data.get("HotelName", None),
            "hotel_formerly_name": hotel_data.get("HotelName", None),
            "destination_code": None,
            "country_code":  hotel_data.get("CountryCode", None),
            "brand_text": None,
            "property_type": vervotech_hotel_data.get("PropertyType", None),
            "star_rating": hotel_data.get("HotelRating", None),
            "chain": vervotech_hotel_data.get("ChainName", None),
            "brand": vervotech_hotel_data.get("BrandName", None),
            "logo": None,
            "primary_photo": hotel_data.get("Images", None)[0],
            
            "review_rating": {
                    "source": "Expedia.com",
                    "number_of_reviews": None,
                    "rating_average": hotel_data.get("HotelRating", None),
                    "popularity_score": None
                },
            "policies": {
                "checkin": {
                    "begin_time": hotel_data.get("CheckInTime", None),
                    "end_time": None,
                    "instructions": None,
                    "special_instructions": None,
                    "min_age":  None,
                    },
                "checkout": {
                    "time": hotel_data.get("CheckOutTime", None),
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
                "latitude": vervotech_hotel_data.get("Latitude", None),
                "longitude": vervotech_hotel_data.get("Longitude", None),
                "address_line_1": vervotech_hotel_data.get("AddressLine1", None),
                "address_line_2": vervotech_hotel_data.get("AddressLine2", None),
                "city": vervotech_hotel_data.get("CityName", None),
                "state": vervotech_hotel_data.get("StateName", None),
                "country": vervotech_hotel_data.get("CountryName", None),
                "country_code": vervotech_hotel_data.get("CountryCode", None),
                "postal_code": vervotech_hotel_data.get("PostalCode", None),
                "full_address": f"{vervotech_hotel_data.get("AddressLine1", None),}, {vervotech_hotel_data.get("AddressLine2", None)}",
                "google_map_site_link": google_map_site_link,
                "local_lang": {
                    "latitude": vervotech_hotel_data.get("Latitude", None),
                    "longitude": vervotech_hotel_data.get("Longitude", None),
                    "address_line_1": vervotech_hotel_data.get("AddressLine1", None),
                    "address_line_2": vervotech_hotel_data.get("AddressLine2", None),
                    "city": vervotech_hotel_data.get("CityName", None),
                    "state": vervotech_hotel_data.get("StateName", None),
                    "country": vervotech_hotel_data.get("CountryName", None),
                    "country_code": vervotech_hotel_data.get("CountryCode", None),
                    "postal_code": vervotech_hotel_data.get("PostalCode", None),
                    "full_address": f"{vervotech_hotel_data.get("AddressLine1", None),}, {vervotech_hotel_data.get("AddressLine2", None)}",
                    "google_map_site_link": google_map_site_link,
                    },
                "mapping": {
                    "continent_id": None,
                    "country_id": hotel_data.get("CountryCode", None),
                    "province_id": None,
                    "state_id": None,
                    "city_id": None,
                    "area_id": None
                    }
                },

            "contacts": {
                "phone_numbers": hotel_data.get("PhoneNumber", None),
                "fax": vervotech_hotel_data.get("Fax", None),
                "email_address": formatted_email,
                "website": vervotech_hotel_data.get("Website", None),
                },
            
            "descriptions": description_info,
            "room_type": None,
            "spoken_languages": None,
            "amenities": None,
            "facilities": formatted_facilities,
            "hotel_photo": hotel_photos_all, 

            "point_of_interests": formatted_pointOfInterests,
            "nearest_airports": formatted_airports, 
            "train_stations": formatted_train_stations,
            "connected_locations": formatted_connected_locations,
            "stadiums": formatted_stadiums
                
        }


        return specific_data


# content_expedia = {}
# hotel_content = HotelContentTBO(content_expedia=content_expedia)


# data = '1000000'
# # hotel_data = hotel_content.hotel_details(hotel_id=data)
# # print(json.dumps(hotel_data, indent=4))

# # itt_hotel_content_json = hotel_content.iit_hotel_content(hotel_id=data)
# # print(json.dumps(itt_hotel_content_json, indent=4))


# vervotech_hotel_data = hotel_content.get_content_by_provider_hotel_ids(hotel_id=data)
# print(json.dumps(vervotech_hotel_data, indent=4))




# def save_json_to_folder(data, hotel_id, folder_name):
#     if not os.path.exists(folder_name):
#         os.makedirs(folder_name)
    
#     file_path = os.path.join(folder_name, f"{hotel_id}.json")
#     try:
#         with open(file_path, "w") as json_file:
#             json.dump(data, json_file, indent=4)
#         print(f"Data saved to {file_path}")
#     except TypeError as e:
#         print(f"Serialization error: {e}")
#     except Exception as e:
#         print(f"An error occurred: {e}")


def get_provider_hotel_id_list(engine, table, providerFamily):
    query = f"SELECT ProviderHotelId FROM {table} WHERE ProviderFamily = '{providerFamily}';"
    df = pd.read_sql(query, engine)
    data = df['ProviderHotelId'].tolist()
    return data


# providerFamily = "TBO"
# get_provider_ids = get_provider_hotel_id_list(engine=engine, table=table, providerFamily=providerFamily)

# # print(get_provider_ids)



# folder_name = "../HotelInfo/TBO"



# for id in get_provider_ids:
#     try:
#         print(id)

#         content_expedia = {}
#         hotel_content = HotelContentTBO(content_expedia=content_expedia)

#         data = hotel_content.iit_hotel_content(hotel_id=id)
#         if data is None:
#             continue

#         save_json_to_folder(data=data, hotel_id=id, folder_name=folder_name)
#         print(f"Completed Createing Json file for hotel: {id}")
    
#     except ValueError:
#         print(f"Skipping invalid id: {id}")













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











def save_json_files_follow_systemId(folder_path, tracking_file_path, engine):
    """
    Save JSON files for each SystemId and keep the tracking file updated.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    table = "vervotech_mapping"
    providerFamily = "TBO"

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
                write_tracking_file(tracking_file_path, remaining_ids)
                continue


            content_expedia = {}
            hotel_content = HotelContentTBO(content_expedia=content_expedia)

            data_dict = hotel_content.iit_hotel_content(hotel_id=systemid)

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



folder_path = '../HotelInfo/TBO'
tracking_file_path = 'tracking_file_for_tbo_content_create.txt'
# table = paximum_table
# engine = local_engine

save_json_files_follow_systemId(folder_path, tracking_file_path, engine)
