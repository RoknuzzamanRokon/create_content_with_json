import math
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd

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



# def print_coordinates(lat, lon, distance):
#     result = calculate_new_coordinates(lat, lon, distance)
#     print(f"North ({distance} km):")
#     print(f"Latitude: {result['north'][0]}")
#     print(f"Longitude: {result['north'][1]}\n")
    
#     print(f"South ({distance} km):")
#     print(f"Latitude: {result['south'][0]}")
#     print(f"Longitude: {result['south'][1]}\n")
    
#     print(f"East ({distance} km):")
#     print(f"Latitude: {result['east'][0]}")
#     print(f"Longitude: {result['east'][1]}\n")
    
#     print(f"West ({distance} km):")
#     print(f"Latitude: {result['west'][0]}")
    # print(f"Longitude: {result['west'][1]}\n")



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
distance_km = input("You are looking for a hotel within walking distance of KM:  ")



bounds = calculate_new_coordinates(latitude, longitude, int(distance_km))
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

# Print the hotels
if not hotels.empty:
    print("Hotels within the given range:")
    print(hotels)
else:
    print("No hotels found within the given range.")