from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:@localhost/csvdata01_02102024")
engine = create_engine(DATABASE_URL)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

# Define table
table = Table("innova_hotels_main", metadata, autoload_with=engine)

def get_provider_hotel_id_list(engine, table, supplierCode):
    query = f"SELECT DISTINCT HotelId FROM {table} WHERE SupplierCode = '{supplierCode}';"
    df = pd.read_sql(query, engine)
    data = df['HotelId'].tolist()
    return data

def initialize_tracking_file(file_path, systemid_list):
    """
    Initializes the tracking file with all SystemIds if it doesn't already exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")

def list_json_file(directory, output_file):
    try:
        # List all JSON files in the directory
        json_files = [f[:-5] for f in os.listdir(directory) if f.endswith('.json')]

        # Write the JSON file names to the output file
        with open(output_file, 'w') as file:
            file.write("\n".join(json_files) + "\n")
        print(f"File list has been written to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_unique_entries(file1_path, file2_path, output):
    # Read the tracking files and calculate unique entries
    with open(file1_path, 'r') as file1:
        file1_data = {line.strip() for line in file1}

    with open(file2_path, 'r') as file2:
        file2_data = {line.strip().rstrip(',') for line in file2}

    unique_to_file2 = file1_data - file2_data

    # Write unique entries to the final output file
    with open(output, 'w') as file3:
        file3.write('\n'.join(unique_to_file2))

    print(f"Unique items written to {output}")

# Main execution
supplierCode = "TBO"
directory = "D:/Rokon/content_for_hotel_json/HotelInfo/TBO"
file_path = "tbo_done_content_creations_tracking_file.txt"

# Step 1: List JSON files and write to the output file
list_json_file(directory=directory, output_file=file_path)

# Step 2: Get system IDs from database and initialize the tracking file
systemid_list = get_provider_hotel_id_list(engine=engine, table=table, supplierCode=supplierCode)
print(f"Total System IDs fetched: {len(systemid_list)}")

tracking_file_path = 'tracking_file_for_iit_table_done_file_name.txt'
initialize_tracking_file(tracking_file_path, systemid_list)

# Step 3: Compare tracking files and write unique entries to the final output file
file1_path = "D:/Rokon/hotels_content_to_create_json_file/TBO/tracking_file_for_iit_table_done_file_name.txt"
file2_path = "D:/Rokon/hotels_content_to_create_json_file/TBO/tbo_done_content_creations_tracking_file.txt"
output = "final_tbo_iit_table_file_path.txt"

get_unique_entries(file1_path, file2_path, output)
