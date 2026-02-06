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
    print(f"Database {DB_FILE} initialized.")

def migrate_csv():
    if not os.path.exists(CSV_FILE):
        print(f"Error: {CSV_FILE} not found.")
        return

    df = pd.read_csv(CSV_FILE)
    
    # Rename columns to match new schema if necessary
    # Expected CSV cols: model, price
    # New Schema cols: product_code, description, price
    
    # Map CSV columns to Schema columns
    df_db = pd.DataFrame()

    # Handle various casing or whitespace in headers just in case
    df.columns = [c.lower().strip() for c in df.columns]

    if 'model' in df.columns:
        df_db['product_code'] = df['model'].astype(str).str.strip()
        # Use Model as description since we don't have a separate category anymore
        df_db['description'] = df['model'].astype(str).str.strip() 
    elif 'product_code' in df.columns: # Fallback support
        df_db['product_code'] = df['product_code'].astype(str).str.strip()
        df_db['description'] = df.get('category', df_db['product_code']) # Fallback to code if category missing
    else:
        print(f"Error: Could not find 'model' or 'product_code' in CSV columns: {df.columns}")
        return

    if 'price' in df.columns:
        df_db['price'] = df['price'].fillna(0.0)
    else:
        print("Error: 'price' column missing.")
        return
    
    # Default Quantity to 0 as it's not in the new simple CSV
    if 'quantity' in df.columns:
        df_db['quantity'] = df['quantity'].fillna(0).astype(int)
    else:
        df_db['quantity'] = 0
    
    # Deduplicate by product_code (keep last)
    df_db = df_db.drop_duplicates(subset=['product_code'], keep='last')
    
    df_db['final_amount'] = 0.0 # Default initialization
    df_db['last_updated_at'] = datetime.datetime.now()
    
    conn = sqlite3.connect(DB_FILE)
    
    try:
        df_db.to_sql('master_data', conn, if_exists='append', index=False)
        print(f"Successfully migrated {len(df_db)} records to SQLite.")
    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    migrate_csv()
