import sqlite3
import pandas as pd
import os
import datetime

# Configuration
CSV_FILE = 'price_database.csv'
DB_FILE = 'enterprise_data.db'

def init_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE) # Clean slate for migration
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Enable Foreign Keys
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # 1. Master Data Table
    # product_code is the PK.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS master_data (
        product_code TEXT PRIMARY KEY,
        description TEXT,
        quantity INTEGER DEFAULT 0,
        price DECIMAL(10, 2),
        final_amount DECIMAL(15, 2),
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # 2. Audit Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS update_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_id TEXT,
        details TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    conn.commit()
    conn.close()
def migrate_csv():
    if not os.path.exists(CSV_FILE):
        print(f"Error: {CSV_FILE} not found.")
        return

    df = pd.read_csv(
        CSV_FILE,
        engine="python",
        on_bad_lines="skip"
    )

    # Normalize headers
    df.columns = [c.lower().strip() for c in df.columns]

    df_db = pd.DataFrame()

    if 'model' in df.columns:
        df_db['product_code'] = df['model'].astype(str).str.strip()
        df_db['description'] = df_db['product_code']
    elif 'product_code' in df.columns:
        df_db['product_code'] = df['product_code'].astype(str).str.strip()
        df_db['description'] = df_db['product_code']
    else:
        print(f"Error: No model/product_code column found: {df.columns}")
        return

    if 'price' not in df.columns:
        print("Error: price column missing.")
        return

    # Clean price (remove commas, text → numeric)
    df_db['price'] = (
        df['price']
        .astype(str)
        .str.replace(',', '', regex=False)
        .str.extract(r'(\d+\.?\d*)')[0]
        .fillna(0)
        .astype(float)
    )

    df_db['quantity'] = 0
    df_db['final_amount'] = 0.0
    df_db['last_updated_at'] = datetime.datetime.now()

    # Deduplicate
    df_db = df_db.drop_duplicates(subset=['product_code'], keep='last')

    conn = sqlite3.connect(DB_FILE)
    try:
        df_db.to_sql('master_data', conn, if_exists='append', index=False)
        print(f"Successfully migrated {len(df_db)} records.")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    migrate_csv()
