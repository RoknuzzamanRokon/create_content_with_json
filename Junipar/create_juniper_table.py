import requests
import xmltodict
from sqlalchemy import create_engine, MetaData, Table, insert
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import sys


# Load environment variables
load_dotenv()

juniper_pass = os.getenv("JUNIPER_PASS")
juniper_mail = os.getenv("JUNIPER_EMAIL")

# Configure logging
logging.basicConfig(
    filename="create_juniper_table.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

# Access the 'juniper' table
juniper_table = metadata.tables.get('juniper')

# SOAP request details
url = "https://xml-uat.bookingengine.es/WebService/jp/operations/staticdatatransactions.asmx"

def get_payload(page, password, email):
    """Generate SOAP payload for a specific page."""
    return f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://www.juniper.es/webservice/2007/">
       <soapenv:Header/>
       <soapenv:Body>
          <HotelPortfolio>
             <HotelPortfolioRQ Version="1.1" Language="en" Page="{page}" RecordsPerPage="100">
                <Login Password="{password}" Email="{email}"/>
             </HotelPortfolioRQ>
          </HotelPortfolio>
       </soapenv:Body>
    </soapenv:Envelope>
    """

headers = {
    'Content-Type': 'text/xml;charset=UTF-8',
    'SOAPAction': '"http://www.juniper.es/webservice/2007/HotelPortfolio"'
}

# Initial variables
current_page = 1
total_pages = None

logger.info("Starting the process to fetch hotel data.")

# Loop through all pages
while total_pages is None or current_page <= total_pages:
    logger.info(f"Processing page {current_page}...")
    try:
        response = requests.post(
            url,
            headers=headers,
            data=get_payload(page=current_page, password=juniper_pass, email=juniper_mail)
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"HTTP request failed for page {current_page}: {e}")
        break

    try:
        data_dict = xmltodict.parse(response.text)
    except Exception as e:
        logger.error(f"Failed to parse XML response for page {current_page}: {e}")
        break

    # Get the total pages on the first request
    if total_pages is None:
        try:
            total_pages = int(
                data_dict.get("soap:Envelope", {})
                .get("soap:Body", {})
                .get("HotelPortfolioResponse", {})
                .get("HotelPortfolioRS", {})
                .get("HotelPortfolio", {})
                .get("@TotalPages", "0")
            )
            logger.info(f"Total pages to process: {total_pages}")
        except Exception as e:
            logger.error(f"Failed to retrieve total pages: {e}")
            break

    # Extract hotel data
    hotel_list = data_dict.get("soap:Envelope", {}).get("soap:Body", {}).get(
        "HotelPortfolioResponse", {}
    ).get("HotelPortfolioRS", {}).get("HotelPortfolio", {}).get("Hotel", [])

    if not hotel_list:
        logger.warning(f"No hotels found on page {current_page}.")
        current_page += 1
        continue

    # Insert hotel data into the database
    with engine.connect() as conn:
        for hotel in hotel_list:
            city_name = hotel.get("City", {}).get("#text", None)
            hotel_data = {
                "HotelName": hotel.get("Name", None),  
                "HotelId": hotel.get("@JPCode", None),
                "ZipCode": hotel.get("ZipCode", None),
                "Latitude": hotel.get("Latitude", None),
                "Longitude": hotel.get("Longitude", None),
                "City": city_name,
            }

            # Insert into the database
            try:
                stmt = insert(juniper_table).values(hotel_data)
                conn.execute(stmt)
                conn.commit()
                logger.info(f"Inserted: {hotel_data}")
            except Exception as e:
                logger.error(f"Error inserting {hotel_data}: {e}")

    current_page += 1

logger.info("All pages processed.")
sys.exit()