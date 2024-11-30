import math
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np

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
    SELECT hotel_latitude, hotel_longitude
    FROM {table} 
    WHERE ProviderHotelId = %s AND ProviderFamily = %s
    """
    
    try:
        df = pd.read_sql(query, engine, params=(hotel_id, supplier_code))
        
        df = df.dropna()
        df['hotel_latitude'] = df['hotel_latitude'].astype(float)
        df['hotel_longitude'] = df['hotel_longitude'].astype(float)
        
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


print(f"You choice '{supplier_code}' supllyer and hotel id is: {hotel_id}")
print(f"This hotel longitude is: {data[0][1]} and latitude is: {data[0][0]}")
print("\n")

latitude = data[0][0]
longitude = data[0][1]
distance_km = input("You are looking for a hotel within walking distance of Kilometer:  ")
print("\n")

bounds = calculate_new_coordinates(latitude, longitude, float(distance_km))
north = bounds['north'][0]
south = bounds['south'][0]
east = bounds['east'][1]
west = bounds['west'][1]

def get_hotels_within_bounds(north, south, east, west, table, engine):
    query = f"""
    SELECT ProviderHotelId, ProviderFamily, hotel_city, hotel_name, hotel_country, country_code, hotel_longitude,
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
    
    print("👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇")
    print(f"Total Hotels = {total_hotels}")
    print(f"Total Suppliers = {total_suppliers}")
    print("\n\n")
    print("All find Hotel Details:")
    print(hotels)
    print("\n\n")
    

    duplicate_hotels = hotels[hotels.duplicated(subset=['hotel_latitude', 'hotel_longitude'], keep=False)]
    
    if not duplicate_hotels.empty:
        print("\nExact Duplicate Hotels:")
        print(duplicate_hotels)
    else:
        print("No duplicate hotels found.")
    
else:
    print("No hotels found within the given range.")





# Ensure latitude and longitude columns are numeric
hotels['hotel_latitude'] = pd.to_numeric(hotels['hotel_latitude'], errors='coerce')
hotels['hotel_longitude'] = pd.to_numeric(hotels['hotel_longitude'], errors='coerce')

hotels = hotels.dropna(subset=['hotel_latitude', 'hotel_longitude'])
def find_near_duplicates(df, lat_col='hotel_latitude', lon_col='hotel_longitude', threshold=0.0001):
    possible_mismatches = []
    coords = df[[lat_col, lon_col]].to_numpy()
    
    for i, (lat1, lon1) in enumerate(coords):
        for j, (lat2, lon2) in enumerate(coords):
            if i != j:  
                lat_diff = abs(lat1 - lat2)
                lon_diff = abs(lon1 - lon2)
                
                if lat_diff == 0 and lon_diff == 0:
                    continue
                
                if lat_diff <= threshold and lon_diff <= threshold:
                    possible_mismatches.append({
                        'Row 1': df.iloc[i].to_dict(),
                        'Row 2': df.iloc[j].to_dict(),
                        'Latitude Difference': lat_diff,
                        'Longitude Difference': lon_diff
                    })
    return possible_mismatches

threshold = 0.0001
near_duplicates = find_near_duplicates(hotels)

if near_duplicates:
    print("\nPossible Hotel Mismatches (Minor Differences in Coordinates):")
    for mismatch in near_duplicates:
        print(f"Row 1: {mismatch['Row 1']}")
        print(f"Row 2: {mismatch['Row 2']}")
        print(f"Latitude Difference: {mismatch['Latitude Difference']:.8f}")
        print(f"Longitude Difference: {mismatch['Longitude Difference']:.8f}\n")
else:
    print("\nNo possible hotel mismatches found.")





