from sqlalchemy import create_engine, text, exc
from sqlalchemy.exc import SQLAlchemyError
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

SERVER_DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(SERVER_DATABASE_URL)


def insert_data_in_chunks(engine, chunk_size=100):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    select_query = text("""
        SELECT * FROM innova_hotels_main
        WHERE SupplierCode =  'ratehawkhotel'
    """)

    insert_query = text("""
        INSERT INTO vervotech_mapping (
            last_update, VervotechId, ProviderHotelId, ProviderFamily, status,
            ModifiedOn, hotel_city, hotel_name, hotel_country, hotel_longitude,
            hotel_latitude, country_code, content_update_status, created_at
        )
        VALUES (
            :last_update, :VervotechId, :ProviderHotelId, :ProviderFamily, :status,
            :ModifiedOn, :hotel_city, :hotel_name, :hotel_country, :hotel_longitude,
            :hotel_latitude, :country_code, :content_update_status, :created_at
        )
    """)

    try:
        with engine.begin() as connection:
            result = connection.execute(select_query).mappings().all()

            if not result:
                print("No records found to transfer.")
                return

            for i in range(0, len(result), chunk_size):
                chunk = result[i:i + chunk_size]
                print(f"Processing chunk {i // chunk_size + 1}...")

                for row in chunk:
                    connection.execute(insert_query, {
                        'last_update': current_time,
                        'VervotechId': None, 
                        'ProviderHotelId': row['HotelId'],
                        'ProviderFamily': row['SupplierCode'],
                        'status': "Update",
                        'ModifiedOn': current_time,
                        'hotel_city': row['City'],
                        'hotel_name': row['HotelName'],
                        'hotel_country': row['Country'],
                        'hotel_longitude': row['Longitude'],
                        'hotel_latitude': row['Latitude'],
                        'country_code': row['CountryCode'],
                        'content_update_status': "Done"
                    })

                print(f"Chunk {i // chunk_size + 1} inserted successfully.")

    except SQLAlchemyError as e:
        print(f"Error during database operation: {e}")

# Call the function to process data in chunks
insert_data_in_chunks(server_engine, chunk_size=100)
