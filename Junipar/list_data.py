import requests
import xmltodict
from sqlalchemy import create_engine, MetaData, Table, insert
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import sys
import pandas as pd

# Load environment variables
load_dotenv()

juniper_pass = os.getenv("JUNIPER_PASS")
juniper_mail = os.getenv("JUNIPER_EMAIL")


logging.basicConfig(
    filename="juniper_list_data_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)



DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)


juniper_table = metadata.tables.get('juniper')


def get_hotel_id_list(table, engine):
    try:
        query = f"SELECT DISTINCT HotelId FROM {table}"
        df = pd.read_sql(query, engine)
        list = df['HotelId'].dropna().tolist()
        return list
    except Exception as e:
        print(f"Error {e}")

def read_tracking_file(file_path):
    """
    Reads tracking file and returns a list of cities yet to be processed.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file.readlines()]
    return []

def write_tracking_file(file_path, cities):
    """
    Writes the remaining cities to the tracking file.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        file.write("\n".join(cities))


