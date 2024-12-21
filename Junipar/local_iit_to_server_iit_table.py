from sqlalchemy import create_engine, Table, MetaData, insert, select
from sqlalchemy.exc import SQLAlchemyError
import os 
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.ERROR, filename='error_log.log')

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

LOCAL_DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
local_engine = create_engine(LOCAL_DATABASE_URL)

SERVER_DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(SERVER_DATABASE_URL)


def fetch_and_insert_data():
    local_connection = local_engine.connect()
    server_connection = server_engine.connect()

    metadata = MetaData()

    table_main_local = Table('innova_hotels_main', metadata, autoload_with=local_engine)

    query = select(table_main_local).where(table_main_local.c.SupplierCode == "Juniper Hotel")
    try:
        result = local_connection.execute(query).fetchall()
        print(f"Fetched {len(result)} records from the local database.")

        if result:
            server_table = Table('innova_hotels_main', metadata, autoload_with=server_engine)
            CHUNK_SIZE = 100
            total_inserted = 0

            for i in range(0, len(result), CHUNK_SIZE):
                chunk = result[i:i + CHUNK_SIZE]

                # Exclude the 'Id' field from each row
                prepared_chunk = [
                    {key: value for key, value in row._mapping.items() if key != 'Id'} 
                    for row in chunk
                ]
                
                with server_engine.begin() as transaction:
                    try:
                        insert_query = insert(server_table).values(prepared_chunk)
                        transaction.execute(insert_query)
                        total_inserted += len(prepared_chunk)
                        print(f"Inserted {len(prepared_chunk)} records into the server database. Total inserted so far: {i + len(prepared_chunk)}")
                    except SQLAlchemyError as e:
                        logging.error(f"An error occurred: {e}")
                        logging.error(f"Problematic chunk: {prepared_chunk}")
                        transaction.rollback()
                        break
        else:
            print("No data to insert.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        local_connection.close()
        server_connection.close()

fetch_and_insert_data()