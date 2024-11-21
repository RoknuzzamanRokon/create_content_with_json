import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

paximum_token = os.getenv("PAXIMUM_TOKEN")
url = "http://service.stage.paximum.com/v2/api/productservice/getproductInfo"

payload = json.dumps({
  "productType": 2,
  "ownerProvider": 2,
  "product": "231182",
  "culture": "en-US"
})
headers = {
  'Content-Type': 'application/json',
  'Authorization': paximum_token
}

response = requests.request("POST", url, headers=headers, data=payload)


# print(type(response.text))
try:
    response_dict = json.loads(response.text)
    hotel_data = response_dict.get("body", {}).get("hotel", {})
    print(hotel_data)
    print(type(response_dict))
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")