import snowflake.connector
import pandas as pd
import random
import time
from datetime import datetime, timedelta

# --- 1. CONFIGURATION (FILL THESE IN!) ---
SNOWFLAKE_USER = "YOUR_USERNAME_HERE"
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD_HERE"
SNOWFLAKE_ACCOUNT = "YOUR_ACCOUNT_ID_HERE"  # e.g. xy12345.us-east-1

# --- 2. DATA GENERATION LOGIC (Same as before) ---
def generate_fake_data(num_records=100):
    products = {
        'Laptop': 1200, 'Headphones': 200, 'Mouse': 50, 'Keyboard': 80, 
        'Monitor': 300, 'Chair': 150, 'Desk': 250, 'Webcam': 100
    }
    cities = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 'Pune']
    
    data = []
    for i in range(num_records):
        product = random.choice(list(products.keys()))
        price = products[product]
        qty = random.randint(1, 5)
        city = random.choice(cities)
        
        record = (
            f"ORD-{random.randint(10000,99999)}", # Order ID
            f"CUST-{random.randint(1000,9999)}",  # Customer ID
            product,
            "Electronics",
            float(price),
            int(qty),
            float(price * qty),
            datetime.now() - timedelta(days=random.randint(0, 30)),
            city
        )
        data.append(record)
    return data

# --- 3. UPLOAD TO SNOWFLAKE ---
def upload_to_snowflake():
    print("🚀 Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse='COMPUTE_WH',
            database='ECOMMERCE_DB',
            schema='RAW_DATA'
        )
        cursor = conn.cursor()
        
        # Generate Data
        print("📦 Generating 100 fake orders...")
        batch_data = generate_fake_data(100)
        
        # Insert Data
        print("☁️ Uploading to Cloud...")
        insert_query = """
        INSERT INTO SALES_DATA (ORDER_ID, CUSTOMER_ID, PRODUCT_NAME, CATEGORY, PRICE, QUANTITY, TOTAL_PRICE, ORDER_DATE, CITY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, batch_data)
        
        conn.commit()
        print(f"✅ Success! {len(batch_data)} records uploaded to Snowflake.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    upload_to_snowflake()
