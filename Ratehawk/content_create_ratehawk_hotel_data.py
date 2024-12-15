import logging
from sqlalchemy import create_engine, Table, MetaData, insert, select
from sqlalchemy.orm import sessionmaker
import pandas as pd
import ast
import sys
import os
from datetime import datetime
import json

logging.basicConfig(
    filename="ratehawk_data_input_local_to_local_table.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)



DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine_L2 = create_engine(DATABASE_URL_LOCAL)
Session_L2 = sessionmaker(bind=local_engine_L2)
session_L2 = Session_L2()
metadata_local_L2 = MetaData()
metadata_local_L2.reflect(bind=local_engine_L2)
ratehawk = Table('ratehawk', metadata_local_L2, autoload_with=local_engine_L2)


local_engine_L1 = create_engine(DATABASE_URL_LOCAL)
Session_L1 = sessionmaker(bind=local_engine_L1)
session_L1 = Session_L1()
metadata_local_L1 = MetaData()
metadata_local_L1.reflect(bind=local_engine_L1)
innova_hotels_main = Table('innova_hotels_main', metadata_local_L1, autoload_with=local_engine_L1)


def get_all_hotel_id_list_with_supplier(supplier, engine, table):
    try:
        query = select(table.c.HotelId).where(table.c.SupplierCode == supplier)
        
        with engine.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        hotel_data_list = df['HotelId'].tolist()
        return hotel_data_list
    except Exception as e:
        logging.error(f"Error fetching Hotel IDs for supplier {supplier}: {e}")
        return None


def create_content_follow_hotel_id(hotel_id):
    try:
        logging.info("Starting data transfer...")
        total_rows_query = session_L2.query(ratehawk).count()
        logging.info(f"Total rows to process: {total_rows_query}")

        batch_size = 50
        total_batches = (total_rows_query // batch_size) + (1 if total_rows_query % batch_size > 0 else 0)
        logging.info(f"Batch size: {batch_size}, Total batches: {total_batches}")

        for batch_number in range(total_batches):
            offset = batch_number * batch_size
            query = session_L2.query(ratehawk).offset(offset).limit(batch_size).statement
            df = pd.read_sql(query, local_engine_L2)
            rows = df.astype(str).to_dict(orient="records")
            logging.debug(f"Fetched {len(rows)} rows for batch {batch_number + 1}")

            with session_L2.begin():
                for idx, row_dict in enumerate(rows, start=1):
                    keys_to_extract = [
                        "address", "id", "images", "kind", "latitude", "longitude", "name",
                        "phone", "postal_code", "region", "star_rating", "email", "amenity_groups"
                    ]
                    filtered_row_dict = {key: row_dict.get(key, None) for key in keys_to_extract}


                    createdAt = datetime.now()
                    createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
                    created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
                    timeStamp = int(created_at_dt.timestamp())


                    address_line_1 = filtered_row_dict.get("address"),
       
                    hotel_name = filtered_row_dict.get("name"),

                    address_query = f"{address_line_1}, {hotel_name}"
                    google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None


                    try:
                        region = ast.literal_eval(filtered_row_dict.get("region", "{}"))
                    except (ValueError, SyntaxError):
                        logging.warning(f"Error parsing 'region' field for row {idx} in batch {batch_number + 1}")
                        region = {}



                    try:
                        amenity_groups = ast.literal_eval(filtered_row_dict.get("amenity_groups", "[]"))
                    except (ValueError, SyntaxError):
                        logging.warning(f"Error parsing 'amenity_groups' field for row {idx} in batch {batch_number + 1}")
                        amenity_groups = []

                    amenity_all = []
                    if amenity_groups:
                        for group in amenity_groups:
                            amenities = group.get("amenities", [])
                            for amenity in amenities:
                                formatted_amenity = {
                                    "type": amenity.get("type", None),
                                    "title": amenity.get("title", None),
                                    "icon": amenity.get("icon", None)
                                }
                                amenity_all.append(formatted_amenity)
                    else:
                        logging.info(f"No amenities found for row {idx} in batch {batch_number + 1}")




                    try:
                        images = ast.literal_eval(filtered_row_dict.get("images", "[]"))
                        print(images)
                    except (ValueError, SyntaxError):
                        logging.warning(f"Error parsing 'images' field for row {idx} in batch {batch_number + 1}")
                        images = []

                    hotel_photos = []
                    if images:
                        for img in images:
                            formatted_image = {
                                "picture_id": None,
                                "title": None,
                                "url": img.replace("t/{size}", "t/x500")
                            }
                            hotel_photos.append(formatted_image)
                    else:
                        logging.info(f"No images found for row {idx} in batch {batch_number + 1}")

                    specific_data = {
                            "created": createdAt_str,
                            "timestamp": timeStamp,
                            "hotel_id": filtered_row_dict.get("id", None),
                            "name": filtered_row_dict.get("name"),
                            "name_local": filtered_row_dict.get("name"),
                            "hotel_formerly_name": filtered_row_dict.get("name"),
                            "destination_code": None,
                            "country_code":  region.get("country_code", None),
                            "brand_text": None,
                            "property_type": filtered_row_dict.get("kind"),
                            "star_rating": filtered_row_dict.get("star_rating"),
                            "chain": filtered_row_dict.get("hotel_chain", None),
                            "brand": None,
                            "logo": None,
                            "primary_photo": images[0] if images else None,
                            "review_rating": {
                                "source": None,
                                "number_of_reviews": None,
                                "rating_average": None,
                                "popularity_score": None,
                            },
                            "policies": {
                                "checkin": {
                                    "begin_time": filtered_row_dict.get("check_in_time", None),
                                    "end_time": filtered_row_dict.get("check_out_time", None),
                                    "instructions": None,
                                    "special_instructions": None,
                                    "min_age": None,
                                },
                                "checkout": {
                                    "time": filtered_row_dict.get("check_out_time", None),
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
                                "latitude": filtered_row_dict.get("latitude"),
                                "longitude": filtered_row_dict.get("longitude"),
                                "address_line_1": filtered_row_dict.get("address"),
                                "address_line_2": None,
                                "city": region.get("name", None),
                                "state": None,
                                "country": region.get("country_name", None),
                                "country_code": region.get("country_code", None),
                                "postal_code": filtered_row_dict.get("postal_code", None),
                                "full_address": f"{filtered_row_dict.get("address")}",
                                "google_map_site_link": google_map_site_link,
                                "local_lang": {
                                    "latitude": filtered_row_dict.get("latitude"),
                                    "longitude": filtered_row_dict.get("longitude"),
                                    "address_line_1": filtered_row_dict.get("address"),
                                    "address_line_2":None,
                                    "city": region.get("name", None),
                                    "state": None,
                                    "country": region.get("country_name", None),
                                    "country_code": region.get("country_code", None),
                                    "postal_code": filtered_row_dict.get("postal_code", None),
                                    "full_address": f"{filtered_row_dict.get("address")}",
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
                                "phone_numbers": filtered_row_dict.get("phone"),
                                "fax": None,
                                "email_address": filtered_row_dict.get("email"),
                                "website": None
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
                            "amenities": amenity_all,
                            "facilities": None,
                            "hotel_photo": hotel_photos, 
                            
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
        

def save_json_file_follow_hotelId(folder_path, supplier_name, chunk_size):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    hotel_ids = get_all_hotel_id_list_with_supplier(supplier_name, engine=local_engine_L1, table=innova_hotels_main)

    print(f"Total Hotel IDs found: {len(hotel_ids)}")
    
    # Process hotel IDs in chunks
    for i in range(0, len(hotel_ids), chunk_size):
        chunk = hotel_ids[i:i + chunk_size]  
        print(f"Processing chunk {i // chunk_size + 1} with {len(chunk)} hotel IDs...")
        
        for hotel_id in chunk:
            file_name = f"{hotel_id}.json"
            file_path = os.path.join(folder_path, file_name)

            try:
                if os.path.exists(file_path):
                    print(f"File {file_name} already exists. Skipping...")
                    continue
                
                data_list = create_content_follow_hotel_id(hotel_id=hotel_id)

                if data_list is None:
                    print(f"Data not found for Hotel ID: {hotel_id}. Skipping...")
                    continue
                
                with open(file_path, "w") as json_file:
                    json.dump(data_list, json_file, indent=4)
                print(f"Saved {file_name} in {folder_path}")
            
            except Exception as e:
                print(f"Error processing data for Hotel ID {hotel_id}: {e}")
                continue

        print(f"Completed chunk {i // chunk_size + 1}/{(len(hotel_ids) + chunk_size - 1) // chunk_size}.")
    print("All chunks processed.")


supplier_name = 'ratehawk'
folder_path = '../HotelInfo/Ratehawk'
save_json_file_follow_hotelId(folder_path=folder_path, supplier_name=supplier_name, chunk_size=1000)
