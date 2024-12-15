import requests
import hashlib
import time
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Hotelbeds API credentials
API_KEY = os.getenv("HOTELBEDS_API_KEY")
API_SECRET = os.getenv("HOTELBEDS_API_SECRET")
BASE_URL = os.getenv("HOTELBEDS_BASE_URL")

# Define the batch size (up to 1000 hotels per request)
BATCH_SIZE = 1000


def generate_signature(api_key, secret):
    """Generate the Hotelbeds API signature."""
    timestamp = str(int(time.time()))
    signature_data = f"{api_key}{secret}{timestamp}"
    signature = hashlib.sha256(signature_data.encode('utf-8')).hexdigest()
    return signature


def fetch_hotel_data(from_index, to_index):
    """Fetch a batch of hotel data from the Hotelbeds API."""
    signature = generate_signature(API_KEY, API_SECRET)

    url = f"{BASE_URL}/hotel-content-api/1.0/hotels?fields=all&language=ENG&from={from_index}&to={to_index}&useSecondaryLanguage=false"

    headers = {
        "Content-Type": "application/json",
        "Api-key": API_KEY,
        "X-Signature": signature
    }

    response = requests.get(url, headers=headers, timeout=120)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Received status code {response.status_code}")
        print(f"Response: {response.text}")
        return None


def process_and_save_to_csv(hotel_data, file_name):
    """Process hotel data and save it to a CSV file."""
    if not hotel_data or 'hotels' not in hotel_data:
        print("No hotel data found to process.")
        return

    hotels = hotel_data['hotels']
    df = pd.DataFrame(hotels)

    # Replace missing values with empty strings
    df.fillna(value="", inplace=True)

    try:
        # Append to CSV file
        if not os.path.isfile(file_name):
            df.to_csv(file_name, mode='w', index=False)
            print(f"CSV file created: {file_name}")
        else:
            df.to_csv(file_name, mode='a', header=False, index=False)
            print(f"Data appended to CSV file: {file_name}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")



def dump_hotel_data_to_csv(total_hotels, batch_size, file_name):
    """Dump all hotel data in batches to a CSV file."""
    print("Starting data dump process...")

    for from_index in range(1, total_hotels + 1, batch_size):
        to_index = min(from_index + batch_size - 1, total_hotels)
        print(f"Fetching hotels from {from_index} to {to_index}...")

        hotel_data = fetch_hotel_data(from_index, to_index)

        if hotel_data:
            process_and_save_to_csv(hotel_data, file_name)
        else:
            print(f"Skipping batch from {from_index} to {to_index} due to errors.")

        # Rate-limiting to avoid hitting API request limits
        time.sleep(2)

    print("Data dump process completed.")


if __name__ == "__main__":
    # Set the total number of hotels to fetch (e.g., 173,000)
    TOTAL_HOTELS = 173000

    # File where the data will be saved
    CSV_FILE_NAME = "hotelbeds_data.csv"

    # Start the dump process
    dump_hotel_data_to_csv(total_hotels=TOTAL_HOTELS, batch_size=BATCH_SIZE, file_name=CSV_FILE_NAME)
