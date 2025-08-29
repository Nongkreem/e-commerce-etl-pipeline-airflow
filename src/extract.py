import pandas as pd
import json
import logging

log = logging.getLogger(__name__)

def extract_sales_data(file_path: str) -> pd.DataFrame:
    """
    Extracts sales data from a CSV file.
    """
    try:
        df = pd.read_csv(file_path)
        log.info(f"Extracted {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        log.error(f"Error: {file_path} not found.")
        return pd.DataFrame()
    except Exception as e:
        log.error(f"Error extracting sales data: {e}")
        return pd.DataFrame()

def extract_events_data(file_path: str) -> pd.DataFrame:
    """
    Extracts events data from a JSON file.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        log.info(f"Extracted {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        log.error(f"Error: {file_path} not found.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        log.error(f"Error: Invalid JSON format in {file_path}.")
        return pd.DataFrame()
    except Exception as e:
        log.error(f"Error extracting events data: {e}")
        return pd.DataFrame()

# You can add a function to extract server.log later when you are ready.
