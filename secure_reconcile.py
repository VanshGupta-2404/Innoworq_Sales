import sqlite3
import pandas as pd
import os
import datetime
import uuid
import logging
import argparse
from typing import Optional, Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP

# Configuration
DB_FILE = 'enterprise_data.db'
LOG_DIR = 'logs'

# Setup Logging
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'reconciliation.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DatabaseService:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)

class AuditService:
    def __init__(self, conn):
        self.conn = conn
    
    def log_update(self, upload_id: str, product_code: str, old_val: Dict, new_val: Dict):
        """
        Logs changes to the audit table.
        """
        cursor = self.conn.cursor()
        details = f"Product: {product_code} | "
        
        changes = []
        if old_val.get('price') != new_val.get('price'):
            changes.append(f"Price: {old_val.get('price')} -> {new_val.get('price')}")
        if old_val.get('quantity') != new_val.get('quantity'):
            changes.append(f"Qty: {old_val.get('quantity')} -> {new_val.get('quantity')}")
            
        details += ", ".join(changes)
        
        cursor.execute("""
            INSERT INTO update_audit (upload_id, details)
            VALUES (?, ?)
        """, (upload_id, details))

class ReconciliationEngine:
    def __init__(self):
        self.db = DatabaseService()

    def process_file(self, file_path: str, upload_id: str = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Main entry point for processing an uploaded file.
        Returns (enriched_df, summary_report).
        enriched_df is a SAFE, derived DataFrame suitable for user download.
        """
        if not upload_id:
            upload_id = str(uuid.uuid4())
            
        print(f"Processing Upload ID: {upload_id}")
        
        # 1. Parse File
        try:
            if file_path.endswith('.csv'):
                df_input = pd.read_csv(file_path)
            elif file_path.endswith(('.xls', '.xlsx')):
                df_input = pd.read_excel(file_path)
            else:
                return None, {"error": "Unsupported file format"}
        except Exception as e:
            return None, {"error": f"Failed to parse file: {str(e)}"}

        # Normalize Input Headers for processing, but keep original for output potentially?
        # Actually, let's work on a copy to preserve original structure if needed, 
        # but for simplicity we will normalize standard columns.
        
        # Create a working copy
        df_working = df_input.copy()
        df_working.columns = [c.lower().strip() for c in df_working.columns]
        
        # Mapping aliases
        col_map = {
            'product_id': 'product_code',
            'model': 'product_code',
            'unit_price': 'price',
            'qty': 'quantity'
        }
        df_working.rename(columns=col_map, inplace=True)
        
        if 'product_code' not in df_working.columns:
            return None, {"error": "Missing required column: product_code"}

        summary = {
            "total_rows": len(df_working),
            "matched": 0,
            "skipped": 0,
            "updated_price": 0,
            "updated_quantity": 0,
            "errors": []
        }
        
        # PREPARE ENRICHED OUTPUT
        # Start with original input data to preserve context
        enriched_rows = [] 

        conn = self.db.get_connection()
        conn.isolation_level = None 
        cursor = conn.cursor()
        audit = AuditService(conn)

        try:
            cursor.execute("BEGIN TRANSACTION;")
            
            for index, row in df_working.iterrows():
                # Base enriched row from input
                output_row = df_input.iloc[index].to_dict()
                
                # Default Status
                status = "SKIPPED_INVALID_DATA"
                final_amt = 0.0
                price_used = 0.0
                qty_used = 0
                
                # Normalize product_code
                raw_val = row['product_code']
                p_code = None
                
                if pd.api.types.is_number(raw_val) and pd.notna(raw_val):
                     if float(raw_val).is_integer():
                         p_code = str(int(raw_val))
                     else:
                         p_code = str(raw_val).strip()
                elif pd.notna(raw_val):
                     p_code = str(raw_val).strip()

                if not p_code:
                    # Invalid product code
                    output_row.update({
                        'reconciliation_status': 'SKIPPED_INVALID_ID',
                        'final_amount': 0.0
                    })
                    enriched_rows.append(output_row)
                    summary['skipped'] += 1
                    continue

                new_price = row.get('price')
                new_qty = row.get('quantity')
                
                # 2. MATCH
                cursor.execute("SELECT price, quantity, description FROM master_data WHERE product_code = ?", (p_code,))
                result = cursor.fetchone()
                
                if not result:
                    logging.warning(f"Row {index}: Product {p_code} NOT FOUND. Skipping.")
                    summary['skipped'] += 1
                    output_row.update({
                        'reconciliation_status': 'SKIPPED_NO_MATCH',
                        'final_amount': 0.0
                    })
                    enriched_rows.append(output_row)
                    continue
                
                db_price, db_qty, db_desc = result
                # Handle None/NaN
                db_price = Decimal(str(db_price)) if db_price is not None else Decimal("0.00")
                db_qty = int(db_qty) if db_qty is not None else 0
                
                summary['matched'] += 1
                status = "UPDATED" # Default if matched, assuming we recompute
                
                # 3. UPDATE LOGIC
                updates = {}
                old_values = {'price': float(db_price), 'quantity': db_qty}
                
                updated_price = db_price
                updated_qty = db_qty
                
                # Update Price if present
                if pd.notna(new_price):
                    try:
                        val = Decimal(str(new_price))
                        if val != db_price:
                            updates['price'] = float(val)
                            updated_price = val
                            summary['updated_price'] += 1
                    except:
                        pass # Keep original if invalid

                # Update Quantity if present
                if pd.notna(new_qty):
                    try:
                        val = int(new_qty)
                        if val != db_qty:
                            updates['quantity'] = val
                            updated_qty = val
                            summary['updated_quantity'] += 1
                    except:
                        pass # Keep original

                # 4. COMPUTE final_amount
                final_amt_val = updated_price * updated_qty
                
                # Update DB 'final_amount' as well
                updates['final_amount'] = float(final_amt_val)
                updates['last_updated_at'] = datetime.datetime.now()
                
                # Perform DB Update
                if updates:
                    set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                    values = list(updates.values())
                    values.append(p_code)
                    
                    cursor.execute(f"UPDATE master_data SET {set_clause} WHERE product_code = ?", values)
                    
                    # Log Audit
                    audit.log_update(upload_id, p_code, old_values, updates)
                
                # 5. BUILD OUTPUT ROW (Explicit)
                output_row.update({
                    'reconciliation_status': status,
                    'price_used': float(updated_price),
                    'quantity_used': int(updated_qty),
                    'final_amount': float(final_amt_val)
                })
                enriched_rows.append(output_row)

            conn.commit()
            logging.info(f"Upload {upload_id} processed successfully.")
            
            # Create the DataFrame
            enriched_df = pd.DataFrame(enriched_rows)
            return enriched_df, summary
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Transaction failed for {upload_id}: {e}")
            summary['error_fatal'] = str(e)
            return None, summary
        finally:
            conn.close()

def print_summary(summary):
    print("\n=== Upload Processing Summary ===")
    if 'error' in summary:
        print(f"❌ ERROR: Failed to process file.")
        print(f"Reason: {summary['error']}")
        return

    if 'error_fatal' in summary:
        print(f"❌ CRITICAL ERROR: Transaction Rolled Back.")
        print(f"Reason: {summary['error_fatal']}")
        return

    print(f"Total Rows In File: {summary['total_rows']}")
    print(f"✅ Matched & Processed: {summary['matched']}")
    print(f"🚫 Skipped (No Match):   {summary['skipped']}")
    print(f"💲 Price Updates:       {summary['updated_price']}")
    print(f"📦 Quantity Updates:    {summary['updated_quantity']}")
    
    if summary['errors']:
        print("\nWarnings:")
        for err in summary['errors']:
            print(f" - {err}")
            
    print("\nStatus: SUCCESS (Committed to Database)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Secure Data Reconciliation Engine")
    parser.add_argument("file", help="Path to Excel/CSV file to process")
    args = parser.parse_args()
    
    if not os.path.exists(args.file):
        print(f"Error: File {args.file} not found.") and exit(1)
        
    engine = ReconciliationEngine()
    result = engine.process_file(args.file)
    print_summary(result[1])
