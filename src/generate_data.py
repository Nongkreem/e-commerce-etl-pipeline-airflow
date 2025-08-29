import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import os

os.makedirs('../data', exist_ok=True)

def generate_sales_data(num_records=1000):
    sales_data = []
    start_date = datetime(2025, 8, 1)

    product_price_map = {
        product_id: random.randint(100, 2000)
        for product_id in range(2001, 2011)
    }

    for i in range(1, num_records + 1):
        order_id = i
        customer_id = random.randint(1000, 1500)
        product_id = random.randint(2001, 2010)
        store_id = f"S{random.randint(1, 5):02d}" #ex. S05
        quantity = random.randint(1, 5)
        unit_price = product_price_map[product_id]
        date = (start_date + timedelta(days=i // 100)).strftime('%Y-%m-%d')

        # เพิ่ม dirty data
        if i % 10 == 0:
            customer_id = None
        if i % 20 == 0:
            price = -random.randint(1, 100)
        if i % 30 == 0:
            quantity = "invalid"
        sales_data.append([order_id, customer_id, product_id, store_id, quantity, unit_price, date])

    df = pd.DataFrame(sales_data, columns=['order_id', 'customer_id', 'product_id', 'store_id', 'quantity', 'unit_price', 'date'])
    df.to_csv('../data/sale_data.csv', index=False)
    print(f"Generated sale_data.csv with {len(df)} records.")

def generate_events_data(num_records=500):
    events_data = []
    event_types = ['click', 'view', 'add_to_cart', 'purchase']
    start_time = datetime(2025, 8, 1, 9, 0, 0)

    for i in range(num_records):
        user_id = random.randint(1000, 1500)
        event_type = random.choice(event_types)
        # page = f"product_{random.randint(2001, 2010)}" if 'product' in event_type else 'homepage'
        product_id = random.randint(2001, 2010)
        timestamp = (start_time + timedelta(seconds=i*random.randint(1, 10))).isoformat()
        
        #dirty data
        if i % 15 == 0:
            user_id = None 
        if i % 25 == 0:
            timestamp = "2025-08-01-invalid"
        if i % 35 == 0:
            timestamp = "view"
        
        events_data.append({
            "user_id": user_id,
            "event_type": event_type,
            "product_id": product_id,
            "timestamp": timestamp
        })
    with open('../data/events.json', 'w') as f:
        json.dump(events_data, f, indent=2)
    print(f"Generated events_data.json with {len(events_data)} records.")

def generate_server_log(num_lines=1000):
    log_messages = [
        "User {user_id} accessed /product_{product_id}",
        "Database connection successful",
        "ERROR: Database timeout on order insert",
        "WARN: High CPU usage detected on server {server_id}",
        "INFO: Order {order_id} processed successfully",
        "ERROR: Unable to connect to promotion database"
    ]
    
    start_time = datetime(2025, 8, 1, 9, 0, 0)
    
    with open('../data/server.log', 'w') as f:
        for i in range(num_lines):
            timestamp = (start_time + timedelta(seconds=i*random.randint(1, 5))).strftime('[%Y-%m-%d %H:%M:%S]')
            
            message_template = random.choice(log_messages)
            
            # dirty data
            log_level = "INFO" # Default level
            if i % 50 == 0:
                message_body = "User {} accessed".format(random.randint(1000, 1500)) # Ensure the message content
                log_level = random.choice(["INFO", "WARNING", "ERROR"]) # Random level for dirty log
            else:
                message_body = message_template.format(
                    user_id=random.randint(1000, 1500),
                    product_id=random.randint(2001, 2010),
                    server_id=random.randint(1, 3),
                    order_id=random.randint(1, 1000)
                )
                if "ERROR" in message_body:
                    log_level = "ERROR"
                elif "WARN" in message_body:
                    log_level = "WARNING"
            
            f.write(f"{timestamp} [{log_level}] {message_body}\n")
    print(f"Generated server.log with {num_lines} lines.")

if __name__ == "__main__":
    generate_sales_data()
    generate_events_data()
    generate_server_log()