import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from io import StringIO
import logging
import os
log = logging.getLogger(__name__)

def load_data_to_postgres(df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
    """
    Loads a Pandas DataFrame into a PostgreSQL table.
    
    """
    conn = None
    cur = None
    try:
        host = os.getenv('PG_HOST')
        port = os.getenv('PG_PORT')
        database = os.getenv('PG_DB')
        user = os.getenv('PG_USER')
        password = os.getenv('PG_PASSWORD')

        if not all([host, port, database, user, password]):
            log.error("Missing one or more PostgreSQL connection environment variable")
            raise ValueError("Missing PostgreSQL connection details")
        log.info(f"Attemping to connect to PostgreSQL at {host}:{port}/{database} for loading tabel '{table_name}' with user {user}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        log.info("Connection established successfully for data loading")

         # <<<--- จำลองให้เกิด error เพื่อทดสอบการแจ้งเตือนผ่าน gmail
        # if table_name == "sales_data": # ให้ล้มเหลาะเฉพาะตอนโหลด sales_data
        #     log.error("--- SIMULATING FAILURE FOR EMAIL ALERT TEST ---")
        #     raise ValueError("Simulated failure for email alert testing!")
        # <<<--- สิ้นสุดการจำลอง

        cur = conn.cursor()

        # --- Create table if it doesn't exist (or handle if_exists logic) ---
        if if_exists == 'replace':
            log.info(f"Dropping existing table '{table_name}' for replacement.")
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            conn.commit()

        if table_name == "sales_data":
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    order_id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    product_id INTEGER,
                    store_id VARCHAR(50),
                    quantity INTEGER,
                    unit_price NUMERIC,
                    date TIMESTAMP,
                    total_price NUMERIC
                );
            """)
            df_columns_to_load = ['order_id', 'customer_id', 'product_id', 'store_id', 'quantity', 'unit_price', 'date', 'total_price']

        elif table_name == "events_data":
             cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    user_id INTEGER,
                    event_type VARCHAR(50),
                    product_id INTEGER
                );
            """)
             df_columns_to_load = ['timestamp', 'user_id', 'event_type', 'product_id']

        elif table_name == "server_logs":
             cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    level VARCHAR(20),
                    message TEXT
                );
            """)
             df_columns_to_load = ['timestamp', 'level', 'message']
        else:
            log.error(f"Unknown table_name: {table_name}. Cannot determine schema.")
            raise ValueError(f"Unknown table_name: {table_name}")
        conn.commit()
        log.info(f"Table '{table_name}' checked/created successfully.")

        # --- Efficiently copy data from DataFrame to PostgreSQL ---
        # Filter DataFrame to include only columns relevant for loading (excluding auto-generated 'id')
        df_filtered = df[df_columns_to_load]
        
        csv_buffer = StringIO()
        df_filtered.to_csv(csv_buffer, index=False) 
        csv_buffer.seek(0)
        
        columns_str = ', '.join(df_filtered.columns)
        cur.copy_expert(f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV HEADER", csv_buffer)
        
        conn.commit()
        log.info(f"Loaded {len(df_filtered)} records into '{table_name}' table.")

    except OperationalError as e:
        log.error(f"Error connecting to PostgreSQL for table '{table_name}': {e}")
        if conn:
            conn.rollback() 
        raise
    except Exception as e:
        log.error(f"Error loading data into PostgreSQL for table '{table_name}': {e}")
        if conn:
            conn.rollback() 
        raise
    finally:
        if conn is not None and not conn.closed:
            if cur is not None:
                cur.close()
            conn.close()
            log.info(f"Connection to data-postgres for table '{table_name}' closed.")

