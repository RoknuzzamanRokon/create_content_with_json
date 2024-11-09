import requests
import hashlib
import time

class HotelContentEAN:
    def __init__(self, master_configuration, nsn_content_vervotech):
        self.nsn_master = master_configuration
        self.credentials = master_configuration['supplier']['ean']
        self.vervotech = nsn_content_vervotech

    def hotel_api_authentication(self):
        try:
            api_key = self.credentials['api_key'].strip()
            secret = self.credentials['secret'].strip()
            base_url = self.credentials['base_url'].strip()

            # Get the current time in seconds
            timestamp = int(time.time())

            # Generate the signature (ensure no extra spaces or characters)
            signature = hashlib.sha512(f'{api_key}{secret}{timestamp}'.encode('utf-8')).hexdigest()

            # Correct format for Authorization header
            auth_header = f"EAN APIKey={api_key},Signature={signature},timestamp={timestamp}"

            # Generate and return the necessary data
            data = {
                'apiKey': api_key,
                'secret': secret,
                'timestamp': timestamp,
                'authHeader': auth_header,
                'base_url': base_url
            }
            return data
        except Exception as e:
            print(f"Error in hotel_api_authentication: {e}")

    def hotel_details(self, hotel_id):
        try:
            # Prepare parameters for the API call
            get_fields = {
                'language': "en-US",
                'supply_source': "expedia",
                'property_id': hotel_id
            }

            # Generate the URL query string
            url_generate = '&'.join([f"{key}={value}" for key, value in get_fields.items()])

            # Get authentication data
            credentials_data = self.hotel_api_authentication()
            auth_header = credentials_data['authHeader']
            base_url = credentials_data['base_url']

            # Construct the full API URL
            curl_url = f"{base_url}/v3/properties/content?{url_generate}"

            # Prepare headers for the request
            headers = {
                "Accept": "application/json",
                "Authorization": auth_header,
                "Content-Type": "application/json"
            }

            # Make the GET request to the API
            response = requests.get(curl_url, headers=headers, timeout=100)

            if response.status_code == 200:
                response_data = response.json()
                hotel_data = response_data.get(hotel_id, {})
                return hotel_data
            else:
                print(f"Error: Failed to retrieve data, status code: {response.status_code}")
                print(response.text)  # Print the response for more debugging info
                return {}

        except Exception as e:
            print(f"Error in hotel_details: {e}")

# Example usage
master_configuration = {
    'supplier': {
        'ean': {
            'api_key': '6k0omsgq0bn0b38aif1fmke1sn',
            'secret': 'ltbou97bgv19',
            'base_url': "https://api.ean.com"
        }
    }
}

nsn_content_vervotech = {}  # Add the appropriate value or object

# Initialize the class
hotel_content = HotelContentEAN(master_configuration, nsn_content_vervotech)

# Example hotel_id to fetch details
hotel_id = '100001138'
hotel_details = hotel_content.hotel_details(hotel_id)

# Print the retrieved hotel details
print(hotel_details)
