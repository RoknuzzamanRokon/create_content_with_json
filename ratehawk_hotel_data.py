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

load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)


table='vervotech_mapping'


class HotelContentRatehawk:
    def __init__(self, content_ratehawk):
        load_dotenv()

        self.credentials = {
            'user_name': os.getenv('RATEHAWK_USERNAME'),
            'user_password': os.getenv('RATEHAWK_PASSWORD'),
            'base_url': os.getenv('RATEHAWK_BASE_URL')
        }
        self.ratehawk = content_ratehawk

    def hotel_api_authentication(self):
        try:
            user_name = self.credentials['user_name'].strip()
            user_password = self.credentials['user_password'].strip()
            base_url = self.credentials['base_url'].strip()

            authorization_basic = base64.b64decode(f"{user_name}:{user_password}")

            data = {
                'username': user_name,
                'password': user_password,
                'authorization_basic': authorization_basic,
                'base_url': base_url
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
                "Accept": "application/json",
                "Accept-Encoding": "application/gzip",
                "Authorization": f"Basic {authorization_basic}"
            }

            payload = {
                "id": hotel_id,
                "language": 'en'
            }

            response = requests.post(f"{base_url}/Hoteldetails", headers=headers, json=payload)

            if response.status_code == 200:
                supplier_response_data = response.json()
                return supplier_response_data
                # hotel_data = supplier_response_data.get(Hote)
            else:
                print(f"Failed to fetch hotel details: {response.text}")
        except Exception as e:
            print(f"Error in fetching hotel details: {e}")


content_ratehawk = {}
hotel_content = HotelContentRatehawk(content_ratehawk=content_ratehawk)