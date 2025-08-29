import pandas as pd
import numpy as np
import logging
import re

log = logging.getLogger(__name__)

def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms and cleans the raw sales data.
    """
    log.info("Starting sales data transformation...")

    # --- 1. Handle customer_id (Missing Values) ---
    df['customer_id'] = df['customer_id'].fillna(0).astype(int)

    # --- 2. Clean and Validate 'quantity' ---
    df['quantity_cleaned'] = pd.to_numeric(df['quantity'], errors='coerce')
    median_quantity = df['quantity_cleaned'].median()
    df['quantity_cleaned'].fillna(median_quantity, inplace=True)
    df['quantity_cleaned'] = df['quantity_cleaned'].astype(int)

    # --- 3. Handle 'unit_price' (Negative Values) ---
    # Make sure to handle 'price' column if it was generated incorrectly instead of 'unit_price'
    if 'price' in df.columns and 'unit_price' not in df.columns: # If old mock data created 'price'
        df['unit_price'] = df['price']
        df.drop(columns=['price'], inplace=True)
    
    df['unit_price'] = df['unit_price'].apply(lambda x: max(x, 0))

    # --- 4. Convert 'date' to datetime type ---
    df['date'] = pd.to_datetime(df['date'])

    # --- 5. Create 'total_price' (Sum of unit_price * quantity) ---
    df['total_price'] = df['unit_price'] * df['quantity_cleaned']

    # --- 6. Drop temporary column and rename 'quantity_cleaned' back to 'quantity' ---
    df['quantity'] = df['quantity_cleaned']
    df.drop(columns=['quantity_cleaned'], inplace=True)

    log.info("Sales data transformation complete.")
    return df

def transform_events_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms and cleans the raw events data.
    Expected raw columns: 'user_id', 'event_type', 'product_id', 'timestamp'
    Target DB columns: 'id', 'timestamp', 'user_id', 'event_type', 'product_id'
    """
    log.info("Starting events data transformation...")
    
    # Drop 'page' column if it exists from old mock data, as it's not in the DB schema
    if 'page' in df.columns:
        df.drop(columns=['page'], inplace=True)
        log.warning("Dropped 'page' column from events data as it's not in the target schema.")

    if 'timestamp' in df.columns:
        # Convert timestamp to datetime, coercing errors to NaT (Not a Time)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        # Drop rows where timestamp couldn't be parsed (NaT)
        initial_rows = len(df)
        df.dropna(subset=['timestamp'], inplace=True) 
        if len(df) < initial_rows:
            log.warning(f"Dropped {initial_rows - len(df)} rows from events data due to invalid timestamps.")
    else:
        log.error("Missing 'timestamp' column in events data. Cannot proceed with timestamp-dependent transformations.")
        return pd.DataFrame() # Return empty if critical column is missing

    if 'user_id' in df.columns:
        df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce').fillna(0).astype(int)
    else:
        log.warning("Missing 'user_id' column in events data. Defaulting to 0 for missing values.")
        df['user_id'] = 0 # Ensure column exists if not present from raw

    if 'product_id' in df.columns:
        df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').fillna(0).astype(int)
    else:
        log.warning("Missing 'product_id' column in events data. Defaulting to 0 for missing values.")
        df['product_id'] = 0 # Ensure column exists if not present from raw

    # Ensure final DataFrame columns match expected output for loading
    expected_cols = ['timestamp', 'user_id', 'event_type', 'product_id']
    df = df[expected_cols] # Select and reorder columns
    
    log.info(f"Events data transformation complete. Remaining records: {len(df)}")
    return df

def transform_server_log_data(log_file_path: str) -> tuple[pd.DataFrame, int]:
    
    log.info(f"Starting server log data transformation from {log_file_path}...")
    
    log_entries = []
    log_pattern = re.compile(r'^\[(?P<timestamp_str>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] \[(?P<level>\S+)\] (?P<message>.*)') 
    error_count = 0

    try:
        with open(log_file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                match = log_pattern.match(line.strip())
                if match:
                    entry = match.groupdict()
                    if entry.get('level') == 'ERROR':
                        error_count += 1
                    log_entries.append(entry)
                else:
                    log.warning(f"Could not parse log line {line_num}: {line.strip()}")

        df_logs = pd.DataFrame(log_entries)
        
        # Convert timestamp string -> datetime object
        if 'timestamp_str' in df_logs.columns:
            initial_rows = len(df_logs)
            df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp_str'], errors='coerce')
            df_logs.dropna(subset=['timestamp'], inplace=True)
            if len(df_logs) < initial_rows:
                log.warning(f"Dropped {initial_rows - len(df_logs)} rows from server logs due to invalid timestamps.")
            df_logs.drop(columns=['timestamp_str'], inplace=True)
        else:
            log.error("Missing 'timestamp_str' column after parsing server logs. Returning empty DataFrame.")
            return pd.DataFrame(columns=['timestamp', 'level', 'message'])

        final_columns = ['timestamp', 'level', 'message']
        df_logs = df_logs[[col for col in final_columns if col in df_logs.columns]]

        log.info(f"Server log data transformation complete. Parsed {len(df_logs)} entries.")
        return df_logs, error_count
    except FileNotFoundError:
        log.error(f"Error: Log file {log_file_path} not found.")
        return pd.DataFrame(columns=['timestamp', 'level', 'message']), 0
    except Exception as e:
        log.error(f"Error transforming server log data: {e}")
        return pd.DataFrame(columns=['timestamp', 'level', 'message']), 0