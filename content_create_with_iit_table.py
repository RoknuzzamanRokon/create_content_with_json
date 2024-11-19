from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd
import json
import time
import ast
import os


DATABASE_URL_LOCAL = f"mysql+pymysql://root:@localhost/csvdata01_02102024"
engine_local = create_engine(DATABASE_URL_LOCAL)
Session_local = sessionmaker(bind=engine_local)
session_local = Session_local()


metadata = MetaData()
metadata.reflect(bind=engine_local)

# iit_table = Table('innova_hotels_main', metadata, autoload_with=engine_local)

iit_table = metadata.tables['innova_hotels_main']


def get_all_hotel_id_list_with_supplier(supplier):
    try:
        # query = f"SELECT HotelId FROM {iit_table}  WHERE SupplierCode = {supplier};"
        query = select(iit_table.c.HotelId).where(iit_table.c.SupplierCode == supplier)

        with engine_local.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            # list_data = df.iloc[0]
            hotel_data_list = df['HotelId'].tolist()
        # df = pd.read_sql(query, iit_table)
        return hotel_data_list
    except Exception as e:
        print(f"Error {e}")         
        return None                 

def create_content_follow_hotel_id(hotel_id):
    try:
        query = select(iit_table).where(iit_table.c.HotelId == hotel_id)

        with engine_local.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            hotel_data = df.iloc[0].to_dict()

            # ast_test = ast.literal_eval(hotel_data.get("ModifiedOn","{}"))

            # id = df.get("Id", "NULL")

            createdAt = datetime.now()
            createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
            created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
            timeStamp = int(created_at_dt.timestamp())


            # Create google map site link.
            address_line_1 = hotel_data.get("AddressLine1", None)
            address_line_2 = hotel_data.get("AddressLine2", None)
            hotel_name = hotel_data.get("HotelName", None)
            # city = hotel_data.get("City", "NULL")
            # postal_code = hotel_data.get("ZipCode", "NULL")
            # state = hotel_info.get("address", {}).get("stateName", "NULL")
            # country = hotel_data.get("CountryName", "NULL")

            address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
            google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != "NULL" else "NULL"


            # Hotel amenities here.
            amenities_keys = [key for key in hotel_data.keys() if key.startswith("Amenities_")]
            hotel_amenities = [
                {
                    "type": hotel_data[amenity_key],
                    "title": hotel_data[amenity_key],
                    "icon": None
                } for amenity_key in amenities_keys if hotel_data[amenity_key]
            ]

            specific_data = {
                "created": createdAt_str,
                "timestamp": timeStamp,
                "hotel_id": hotel_data.get("HotelId", None),
                "name": hotel_data.get("HotelName", None),
                "name_local": hotel_data.get("HotelName", None),
                "hotel_formerly_name": hotel_data.get("HotelName", None),
                "destination_code": hotel_data.get("DestinationId", None),
                "country_code":  hotel_data.get("CountryCode", None),
                "brand_text": None,
                "property_type": hotel_data.get("HotelType", None),
                "star_rating": hotel_data.get("HotelStar", None),
                "chain": None,
                "brand": None,
                "logo": None,
                "primary_photo": hotel_data.get("PrimaryPhoto", None),
                "review_rating": {
                    "source": None,
                    "number_of_reviews": hotel_data.get("HotelReview", None),
                    "rating_average": None,
                    "popularity_score": None,
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
                    "nationality_restrictions": None,
                },
                "address": {
                    "latitude": hotel_data.get("Latitude", None),
                    "longitude": hotel_data.get("Longitude", None),
                    "address_line_1": hotel_data.get("AddressLine1", None),
                    "address_line_2": hotel_data.get("AddressLine2", None),
                    "city": hotel_data.get("City", None),
                    "state": hotel_data.get("State", None),
                    "country": hotel_data.get("CountryName", None),
                    "country_code": hotel_data.get("CountryCode", None),
                    "postal_code": hotel_data.get("ZipCode", None),
                    "full_address": f"{hotel_data.get('AddressLine1', None)}, {hotel_data.get('AddressLine2', None)}",
                    "google_map_site_link": google_map_site_link,
                    "local_lang": {
                        "latitude": hotel_data.get("Latitude", None),
                        "longitude": hotel_data.get("Longitude", None),
                        "address_line_1": hotel_data.get("AddressLine1", None),
                        "address_line_2": hotel_data.get("AddressLine2", None),
                        "city": hotel_data.get("City", None),
                        "state": hotel_data.get("State", None),
                        "country": hotel_data.get("CountryName", None),
                        "country_code": hotel_data.get("CountryCode", None),
                        "postal_code": hotel_data.get("ZipCode", None),
                        "full_address": f"{hotel_data.get('AddressLine1', None)}, {hotel_data.get('AddressLine2', None)}",
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
                    "phone_numbers": hotel_data.get("ContactNumber", None),
                    "fax": None,
                    "email_address": hotel_data.get("Email", None),
                    "website": hotel_data.get("Website", None)
                },
                "descriptions": [
                    {
                        "title": None,
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
                "amenities": hotel_amenities,
                "facilities": None,
                "hotel_photo": hotel_data.get("PrimaryPhoto", None), 
                
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
    except Exception as e:
        print(f"Error {e}")



def save_json_file_follow_hotelId(folder_path, supplier_name):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    hotel_ids = get_all_hotel_id_list_with_supplier(supplier=supplier_name)

    print(f"Total Hotel IDs found: {len(hotel_ids)}")

    for hotel_id in hotel_ids:
        file_name = f"{hotel_id}.json"
        file_path = os.path.join(folder_path, file_name)

        try:
            if os.path.exists(file_path):
                print(f"File {file_name} already exists. Skipping..")
                continue
            try:
                data_list = create_content_follow_hotel_id(hotel_id=hotel_id)

                if data_list is None:
                    print(f"Data not found for Hotel: {hotel_id}. Skipping..")
                    continue
                with open(file_path, "w") as json_file:
                    json.dump(data_list, json_file, indent=4)
                print(f"Save {file_name} in {folder_path}")
            except Exception as e:
                print(f"Error Processing data for Hotel Id {hotel_id}: {e}")
                continue
        except Exception as e:
            print(f"Error occurred while checking or creating file for hotel id {hotel_id}: {e}")
            continue
# data = get_all_hotel_id_list_with_supplier(supplier='ratehawk')

# print(data)


# row_data = create_content_follow_hotel_id(hotel_id='6291597')

# print(row_data)

supplier_name = 'ratehawk'
folder_path = './HotelInfo/Ratehawk'
save_json_file_follow_hotelId(folder_path=folder_path, supplier_name=supplier_name)










