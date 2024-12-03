import math
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from difflib import SequenceMatcher

# Load environment variables
load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Database connection
DATABASE_URL_SERVER = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL_SERVER)

table = 'vervotech_mapping'

def calculate_new_coordinates(latitude, longitude, distance_km):
    EARTH_RADIUS_KM = 6371 
    DEGREE_TO_RADIAN = math.pi / 180  
    
    latitude_change = distance_km / 111
    latitude_rad = latitude * DEGREE_TO_RADIAN
    longitude_change = distance_km / (111 * math.cos(latitude_rad))
    
    return {
        'north': (latitude + latitude_change, longitude),
        'south': (latitude - latitude_change, longitude),
        'east': (latitude, longitude + longitude_change),
        'west': (latitude, longitude - longitude_change)
    }

def get_a_location(supplier_code, hotel_id, table, engine):
    query = f"""
    SELECT hotel_latitude, hotel_longitude, hotel_name
    FROM {table} 
    WHERE ProviderHotelId = %s AND ProviderFamily = %s
    """
    
    try:
        df = pd.read_sql(query, engine, params=(hotel_id, supplier_code))
        
        df = df.dropna()
        df['hotel_latitude'] = df['hotel_latitude'].astype(float)
        df['hotel_longitude'] = df['hotel_longitude'].astype(float)
        df['hotel_name'] = df['hotel_name'].astype(str)
        
        # Return as a list of tuples
        return df.to_records(index=False).tolist()
    except Exception as e:
        print(f"Error retrieving location data: {e}")
        return []

# Input values
supplier_code = input("Choice a Supplyer name: ")
hotel_id = input("Give your hotel Id: ")
print("\n\n")

table = "vervotech_mapping"

data = get_a_location(supplier_code, hotel_id, table, engine)

print(data)
print(f"You choice '{supplier_code}' supllyer and hotel id is: {hotel_id}")
print(f"This hotel longitude is: {data[0][1]} and latitude is: {data[0][0]}")
print(f"Hotel name: {data[0][2]}")
print("\n")

def get_similarity(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100

def get_similar_hotels(hotels, target_name, threshold):
    similar_hotels = hotels[hotels['hotel_name'].apply(lambda name: get_similarity(name, target_name) >= threshold)]
    return similar_hotels

def new_func(engine, table, calculate_new_coordinates, data):
    latitude = data[0][0]
    longitude = data[0][1]
    target_name = data[0][2]
    
    # print(f"Hotel name: {target_name}\n")
    
    distance_km = input("You are looking for a hotel within walking distance of Kilometer:  ")
    print("\n")

    bounds = calculate_new_coordinates(latitude, longitude, float(distance_km))
    north = bounds['north'][0]
    south = bounds['south'][0]
    east = bounds['east'][1]
    west = bounds['west'][1]

    def get_hotels_within_bounds(north, south, east, west, table, engine):
        query = f"""
        SELECT ProviderHotelId, ProviderFamily, hotel_city, hotel_name, hotel_country, hotel_longitude,
        hotel_latitude 
        FROM {table}
        WHERE hotel_latitude BETWEEN %s AND %s
          AND hotel_longitude BETWEEN %s AND %s
        """
    
        try:
            df = pd.read_sql(query, engine, params=(south, north, west, east))
            return df
        except Exception as e:
            print(f"Error retrieving hotel data: {e}")
            return pd.DataFrame()

    hotels = get_hotels_within_bounds(north, south, east, west, table, engine)

    if not hotels.empty:
        total_hotels = len(hotels)
        total_suppliers = hotels['ProviderFamily'].nunique()
    
        print("\n\n")
        print("👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇")
        print(f"Total Hotels = {total_hotels}")
        print(f"Total Suppliers = {total_suppliers}")
        print("\n\n")
        print("All Found Hotel Details:")
        print(hotels)
        
        print("\n\n")
        # Check for similar hotels when match 100%
        similar_hotels = get_similar_hotels(hotels, target_name, threshold=100)
        print("Here match 👉👉 100% charecter and show similler name of hotel.")
        total_similar_hotels = len(similar_hotels)
        print(f"Total Similar Hotels: {total_similar_hotels}")
        if not similar_hotels.empty:
            print(similar_hotels)
        else:
            print("No hotels with similar names found.")


        print("\n\n")
        # Check for similar hotels when match 80%
        similar_hotels = get_similar_hotels(hotels, target_name, threshold=80)
        print("Here match 👉👉 80% charecter and show similler name of hotel.")
        total_similar_hotels = len(similar_hotels)
        print(f"Total Similar Hotels: {total_similar_hotels}")
        if not similar_hotels.empty:
            print(similar_hotels)
        else:
            print("No hotels with similar names found.")


        print("\n\n")
        # Check for similar hotels when match 60%
        similar_hotels = get_similar_hotels(hotels, target_name, threshold=60)
        print("Here match 👉👉 60% charecter and show similler name of hotel.")
        total_similar_hotels = len(similar_hotels)
        print(f"Total Similar Hotels: {total_similar_hotels}")
        if not similar_hotels.empty:
            print(similar_hotels)
        else:
            print("No hotels with similar names found.")

    else:
        print("No hotels found within the given range.")


new_func(engine, table, calculate_new_coordinates, data)