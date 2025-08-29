import psycopg2
from psycopg2 import OperationalError
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_data_postgres_connection():
    """
    Tests the connection to the data-postgres database
    บังคับ error เมื่อการเชื่อมต่อกับ DB ผิดพลาด
    """
    conn = None
    try:
        host = os.getenv('PG_HOST')
        port = os.getenv('PG_PORT')
        database = os.getenv('PG_DB')
        user = os.getenv('PG_USER')
        password = os.getenv('PG_PASSWORD')

        logger.info(f"Connecting to PostgreSQL at {host}:{port}/{database}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logger.info("Connection established successfully")
        print("connect db success")

    except OperationalError as e:
        logger.error(f"The connection to datawarehouse failed: {e}")
        print("connect db failed")
        raise # Re-raise the exception to make the Airflow task fail
    finally:
        if conn is not None:
            conn.close()