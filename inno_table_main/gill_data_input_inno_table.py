from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.orm import sessionmaker
import json
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import os
import time

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


# Database setup
DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)

metadata = MetaData()
metadata.reflect(bind=engine)
hotel_info_all = Table('hotel_info_all', metadata, autoload_with=engine)
innova_hotels_main = Table('innova_hotels_main', metadata, autoload_with=engine)

json_mapping = {
    'HotelName': 'name',
    'HotelId': 'systemId',
    'AddressLine1': 'address.line1',
    'AddressLine2': 'address.line2',
    'State': 'address.stateName',
    'StateCode': 'address.stateCode',
    'PostCode': 'address.zipCode',
    'City': 'address.cityName',
    'CityCode': 'address.CityCode',
    'Country': 'address.countryName',
    'CountryCode': 'address.countryCode',
    'Latitude': 'geocode.lat',
    'Longitude': 'geocode.lon',
    'ContactNumber': 'contact.phoneNo',
    'Website': 'contact.website',
    'HotelStar': 'rating',
    'PrimaryPhoto': 'imageUrls',  
    'HotelReview': 'tripAdvisorRating',
    'DestinationId': 'giDestinationId',
}

session = Session()

def escape_single_quotes(value):
    if isinstance(value, str):
        return value.replace("'", "''")
    return value
    
try:
    with session.begin():
        results = (
            session.query(hotel_info_all)
            .filter(hotel_info_all.c.StatusUpdateHotelInfo == 'Done Json')
            .group_by(hotel_info_all.c.GiDestinationId)
            .limit(100) # Set here limit.....................................................
            .all()
        )
        # This section for primary image url.
        for row in results:
            hotel_info = row.HotelInfo
            data = {}

            for target_col, json_key in json_mapping.items():
                if target_col == 'PrimaryPhoto':
                    image_urls = hotel_info.get('imageUrls', [])
                    if image_urls:
                        data['PrimaryPhoto'] = image_urls[0]
                    else:
                        data['PrimaryPhoto'] = None
                else:
                    keys = json_key.split('.')
                    value = hotel_info
                    for key in keys:
                        value = value.get(key) if value else None
                    data[target_col] = value if value is not None else None

            # This section for room amenities.
            amenities = hotel_info.get('masterHotelAmenities', [])
            if amenities:
                for i in range(1, 6):
                    column_name = f"Amenities_{i}"
                    data[column_name] = amenities[i - 1] if i <= len(amenities) else None
            else:
                data['Amenities_1'] = None

            data['SupplierCode'] = 'Oryx'
            data = {k: escape_single_quotes(v) for k, v in data.items()}

            # Set missing columns to NULL
            for col in innova_hotels_main.columns:
                if col.name not in data:
                    data[col.name] = None

            columns = ', '.join(data.keys())
            values = ', '.join([f" '{v}'" if v is not None else 'NULL' for v in data.values()])
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in data.keys()])

            sql = f"""
                INSERT INTO innova_hotels_main ({columns})
                VALUES ({values})
                ON DUPLICATE KEY UPDATE {update_clause}
            """

            session.execute(text(sql))


    print("Data Updated successfully in innova_hotels_main.")
    
except Exception as e:
    session.rollback()
    print(f"An error occurred: {e}")

finally:
    session.close()


























