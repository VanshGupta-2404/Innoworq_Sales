import sqlite3
import pandas as pd
import os
from secure_reconcile import ReconciliationEngine
import tempfile
import uuid

# 1. Verify DB Content
print("=== 1. Checking Database Content ===")
conn = sqlite3.connect('enterprise_data.db')
cursor = conn.cursor()

# Check count
count = cursor.execute("SELECT count(*) FROM master_data").fetchone()[0]
print(f"Total Rows in DB: {count}")

# Check sample
sample_model = "SC8000"
row = cursor.execute("SELECT * FROM master_data WHERE product_code = ?", (sample_model,)).fetchone()
print(f"Sample Row ({sample_model}): {row}")

if not row:
    print("❌ Critical: Sample row missing!")
    exit(1)

# Check Price
price = row[3] # 4th column based on schema: code, desc, qty, price... wait schema order: code, desc, qty, price, final, last
# Schema from migrate_db.py: product_code, description, quantity, price, final_amount, last_updated_at
# Indexes: 0, 1, 2, 3, 4, 5
price = row[3]
if float(price) != 1097000.0:
    print(f"❌ Critical: Price Mismatch! Expected 1097000, got {price}")
else:
    print("✅ Price matches.")

# 2. Verify Reconciliation Logic
print("\n=== 2. Testing Reconciliation Logic ===")
engine = ReconciliationEngine()

# Create a test CSV upload
test_df = pd.DataFrame({
    'model': ['SC8000', 'NON_EXISTENT'],
    'quantity': [5, 10]
})
temp_csv = f"test_verify_{uuid.uuid4()}.csv"
test_df.to_csv(temp_csv, index=False)

try:
    print("Processing test upload...")
    enriched_df, summary = engine.process_file(temp_csv)
    
    print("Summary:", summary)
    
    if summary['matched'] == 1 and summary['skipped'] == 1:
        print("✅ Matching logic working correctly (1 match, 1 skip).")
    else:
        print(f"❌ Matching logic failed. Summary: {summary}")

    # Verify attributes of matched row
    matched_row = enriched_df[enriched_df['reconciliation_status'] == 'UPDATED'].iloc[0]
    # Check if price_used came from DB
    if matched_row['price_used'] == 1097000.0:
         print("✅ Price enrichment working.")
    else:
         print(f"❌ Price enrichment failed. Got {matched_row['price_used']}")

finally:
    if os.path.exists(temp_csv):
        os.remove(temp_csv)
