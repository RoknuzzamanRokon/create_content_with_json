from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
import os
import ast
from dotenv import load_dotenv

load_dotenv()

# Database connection setup
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(DATABASE_URL_SERVER)
Session_1 = sessionmaker(bind=server_engine)
session_1 = Session_1()

DATABASE_URL_LOCAL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(DATABASE_URL_LOCAL)
Session_2 = sessionmaker(bind=local_engine)
session_2 = Session_2()

metadata_local = MetaData()
metadata_server = MetaData()

metadata_local.reflect(bind=local_engine)
metadata_server.reflect(bind=server_engine)

ratehawk = Table('ratehawk', metadata_local, autoload_with=local_engine)
innova_hotels_main = Table('innova_hotels_main', metadata_server, autoload_with=server_engine)


def transfer_all_rows():
    try:
        
        total_rows_query = session_2.query(ratehawk).count()
        batch_size = 10000
        total_batches = (total_rows_query // batch_size) + (1 if total_rows_query % batch_size > 0 else 0)

        for batch_number in range(total_batches):
            offset = batch_number * batch_size
            query = session_2.query(ratehawk).offset(offset).limit(batch_size).statement
            df = pd.read_sql(query, local_engine)
            rows = df.astype(str).to_dict(orient="records")

            with session_1.begin():
                for row_dict in rows:
                    keys_to_extract = [
                        "address", "hid", "images", "kind", "latitude", "longitude", "name", 
                        "phone", "postal_code", "region", "star_rating", "email", "amenity_groups"
                    ]
                    filtered_row_dict = {key: row_dict.get(key, None) for key in keys_to_extract}

                    try:
                        region = ast.literal_eval(filtered_row_dict.get("region", "{}"))
                    except (ValueError, SyntaxError):
                        region = {}

                    try:
                        amenity_groups = ast.literal_eval(filtered_row_dict.get("amenity_groups", "[]"))
                    except (ValueError, SyntaxError):
                        amenity_groups = []

                    try:
                        images = ast.literal_eval(filtered_row_dict.get("images", "[]"))
                    except (ValueError, SyntaxError):
                        images = []

                    if images:
                        images = [image_url.replace("t/{size}", "t/x500") for image_url in images]

                    data = {
                        'HotelId': filtered_row_dict.get("hid", None),
                        'City': region.get("name", None),
                        'Country': region.get("country_name", None),
                        'CountryCode': region.get("country_code", None),
                        'PostCode': filtered_row_dict.get("postal_code", None),
                        'HotelType': filtered_row_dict.get("kind"),
                        'HotelName': filtered_row_dict.get("name"),
                        'Latitude': filtered_row_dict.get("latitude"),
                        'Longitude': filtered_row_dict.get("longitude"),
                        'PrimaryPhoto': images[0] if images else None,
                        'AddressLine1': filtered_row_dict.get("address"),
                        'Email': filtered_row_dict.get("email"),
                        'ContactNumber': filtered_row_dict.get("phone"),
                        'HotelStar': filtered_row_dict.get("star_rating"),
                        'Amenities_1': None,
                        'Amenities_2': None,
                        'Amenities_3': None,
                        'Amenities_4': None,
                        'Amenities_5': None,
                        'SupplierCode': 'Ratehawk'
                    }

                    if amenity_groups:
                        first_group = amenity_groups[0].get("amenities", [])
                        data['Amenities_1'] = first_group[0] if len(first_group) > 0 else None
                        data['Amenities_2'] = first_group[1] if len(first_group) > 1 else None
                        data['Amenities_3'] = first_group[2] if len(first_group) > 2 else None
                        data['Amenities_4'] = first_group[3] if len(first_group) > 3 else None
                        data['Amenities_5'] = first_group[4] if len(first_group) > 4 else None
                    else:
                        data['Amenities_1'] = None
                        data['Amenities_2'] = None
                        data['Amenities_3'] = None
                        data['Amenities_4'] = None
                        data['Amenities_5'] = None

                    stmt = insert(innova_hotels_main).values(data)
                    session_1.execute(stmt)
            print(f"Batch {batch_number + 1} of {total_batches} processed successfully.")

        print("All rows updated successfully in innova_hotels_main.")
    except Exception as e:
        print(f"An error occurred: {e}")
        session_1.rollback()
        session_2.rollback()

transfer_all_rows()
