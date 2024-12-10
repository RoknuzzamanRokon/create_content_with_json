import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

grnconnect_api_key = os.getenv("GRNCONNECT_API_KEY")
grnconnect_base_url = os.getenv("GRNCONNECT_BASE_URL")

class GRNConnectAPI:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url

    def hotel_details(self, hotel_id):
        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "application/gzip",
                "api-key": self.api_key
            }

            # API endpoint
            url = f"{self.base_url}/api/v3/hotels?hcode={hotel_id}&version=2.0"

            # Make the GET request
            response = requests.get(url, headers=headers)
            print(response.text)

            if response.status_code == 200:
                data = response.json()
                print(data)  
                hotel_data = data.get('HotelDetails', [{}])[0] 
                return hotel_data
            else:
                return {
                    "error": f"Failed to fetch hotel details. HTTP Status: {response.status_code}",
                    "response": response.text
                }

        except Exception as e:
            # Handle exceptions
            return {"error": str(e)}

    def iit_hotel_content(hotel_id):
        try:
            

# Initialize the API class
grnconnect = GRNConnectAPI(api_key=grnconnect_api_key, base_url=grnconnect_base_url)

hotel_id = "1000041"  
hotel_details = grnconnect.hotel_details(hotel_id)

# Print the result
print(json.dumps(hotel_details, indent=4))
