import pandas as pd
import os
import getpass
import logging
from tabulate import tabulate

# CONFIGURATION
PRICE_DB_FILE = 'price_database.csv'
LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'admin_actions.log')
ADMIN_PASSWORD = 'admin123'

def setup_logging(log_file_path):
    log_directory = os.path.dirname(log_file_path)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # innovative way to reconfigure logging if already set
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
            
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

setup_logging(LOG_FILE)

def load_price_db():
    if not os.path.exists(PRICE_DB_FILE):
        print("Error: Price database not found.")
        return None
    return pd.read_csv(PRICE_DB_FILE)

def save_price_db(df):
    df.to_csv(PRICE_DB_FILE, index=False)

def log_action(action, details, success=True):
    status = "SUCCESS" if success else "FAILED"
    logging.info(f"{action}: {details} - {status}")

def standard_user_flow():
    print("\n--- Standard User Mode ---")
    data_file = input("Enter path to your dataset (CSV): ").strip()
    
    if not os.path.exists(data_file):
        print(f"Error: File '{data_file}' not found.")
        return

    try:
        user_df = pd.read_csv(data_file)
        if 'Product_Name' not in user_df.columns or 'Quantity' not in user_df.columns:
            print("Error: Dataset must contain 'Product_Name' and 'Quantity' columns.")
            return

        price_db = load_price_db()
        if price_db is None:
            return

        # Merge with price database
        merged_df = pd.merge(user_df, price_db, on='Product_Name', how='inner')
        
        # Calculate totals
        merged_df['Item_Total'] = merged_df['Unit_Price'] * merged_df['Quantity']
        grand_total = merged_df['Item_Total'].sum()

        # Display result (excluding Unit_Price)
        display_df = merged_df[['Product_Name', 'Quantity', 'Item_Total']]
        print("\n--- Processed Data ---")
        print(tabulate(display_df, headers='keys', tablefmt='psql', floatfmt=".2f"))
        print(f"\nGrand Total: {grand_total:.2f}")

    except Exception as e:
        print(f"Error processing file: {e}")

def admin_flow():
    while True:
        print("\n--- Admin Mode ---")
        print("1. View Full Database")
        print("2. Add Product")
        print("3. Update Product Price")
        print("4. Delete Product")
        print("5. View Logs")
        print("6. Exit to Main Menu")
        
        choice = input("Select an option: ").strip()

        if choice == '1':
            df = load_price_db()
            if df is not None:
                print(tabulate(df, headers='keys', tablefmt='psql', floatfmt=".2f"))
        
        elif choice == '2':
            add_product()

        elif choice == '3':
            update_product()

        elif choice == '4':
            delete_product()

        elif choice == '5':
            view_logs()

        elif choice == '6':
            break
        else:
            print("Invalid option.")

def add_product():
    try:
        p_id = int(input("Enter Product ID: "))
        p_name = input("Enter Product Name: ").strip()
        p_price = float(input("Enter Unit Price: "))
        
        df = load_price_db()
        if df is not None:
            if p_id in df['Product_ID'].values:
                print("Error: Product ID already exists.")
                return

            new_row = pd.DataFrame({'Product_ID': [p_id], 'Product_Name': [p_name], 'Unit_Price': [p_price]})
            df = pd.concat([df, new_row], ignore_index=True)
            save_price_db(df)
            log_action("ADD_PRODUCT", f"ID={p_id}, Name={p_name}, Price={p_price}")
            print("Product added successfully.")
            
    except ValueError:
        print("Invalid input.")

def update_product():
    try:
        p_id = int(input("Enter Product ID to update: "))
        df = load_price_db()
        
        if df is not None:
            if p_id not in df['Product_ID'].values:
                print("Error: Product ID not found.")
                return
            
            new_price = float(input("Enter new Unit Price: "))
            df.loc[df['Product_ID'] == p_id, 'Unit_Price'] = new_price
            save_price_db(df)
            log_action("UPDATE_PRICE", f"ID={p_id}, NewPrice={new_price}")
            print("Price updated successfully.")

    except ValueError:
        print("Invalid input.")

def delete_product():
    try:
        p_id = int(input("Enter Product ID to delete: "))
        df = load_price_db()
        
        if df is not None:
            if p_id not in df['Product_ID'].values:
                print("Error: Product ID not found.")
                return
            
            # Security Check
            password = getpass.getpass("Enter Admin Password to confirm deletion: ")
            if password == ADMIN_PASSWORD:
                df = df[df['Product_ID'] != p_id]
                save_price_db(df)
                log_action("DELETE_PRODUCT", f"ID={p_id}", success=True)
                print("Product deleted successfully.")
            else:
                log_action("DELETE_PRODUCT_ATTEMPT", f"ID={p_id} - Incorrect Password", success=False)
                print("Error: Incorrect password. Deletion aborted.")

    except ValueError:
        print("Invalid input.")

def view_logs():
    if os.path.exists(LOG_FILE):
        print("\n--- Admin Logs ---")
        with open(LOG_FILE, 'r') as f:
            print(f.read())
    else:
        print("No logs found.")

def main():
    while True:
        print("\n=== Secure Data-Processing AI ===")
        print("1. Standard User")
        print("2. Admin User")
        print("3. Exit")
        
        choice = input("Select Role: ").strip()

        if choice == '1':
            standard_user_flow()
        elif choice == '2':
            admin_flow()
        elif choice == '3':
            print("Exiting...")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
