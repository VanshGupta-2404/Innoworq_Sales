import os
import csv as import_csv
import pandas as pd
from secure_reconcile import ReconciliationEngine
import tempfile
import uuid

app = Flask(__name__)
app.secret_key = 'super_secure_secret_key'

UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

engine = ReconciliationEngine()

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file:
        # Save temp file
        temp_filename = f"TEMP_{uuid.uuid4()}_{file.filename}"
        temp_filepath = os.path.join(UPLOAD_FOLDER, temp_filename)
        file.save(temp_filepath)
        
        try:
            # Process the file
            enriched_df, summary = engine.process_file(temp_filepath)
            
            if enriched_df is None and 'error' in summary:
                flash(f"Error: {summary['error']}")
                return redirect(url_for('index'))
                
            if 'error_fatal' in summary:
                flash(f"Critical Error: {summary['error_fatal']}")
                return redirect(url_for('index'))
                
            # Generate Output Filename
            # reconciled_<original_filename>_<timestamp>.xlsx
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            original_base = os.path.splitext(file.filename)[0]
            output_filename = f"reconciled_{original_base}_{timestamp}.xlsx"
            output_path = os.path.join(RESULTS_FOLDER, output_filename)
            
            # Save Enriched DataFrame
            enriched_df.to_excel(output_path, index=False)
            
            return send_file(output_path, as_attachment=True)
            
        except Exception as e:
             flash(f"System Error: {str(e)}")
             return redirect(url_for('index'))
        finally:
            # Clean up temp file
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)

@app.route('/add_product', methods=['POST'])
def add_product():
    try:
        p_code = request.form.get('product_code')
        category = request.form.get('category')
        price = request.form.get('price')
        quantity = request.form.get('quantity')
        
        if not all([p_code, category, price, quantity]):
            flash("Error: All fields are required")
            return redirect(url_for('index'))
            
        # 1. Append to CSV
        csv_file = 'price_database.csv'
        # Check if file exists to determine if we need header (though it should exist)
        file_exists = os.path.exists(csv_file)
        
        # Schema: product_code,store_id,store_name,category,quantity,price,currency,upload_batch
        new_row = [
            p_code,
            "MANUAL",
            "Manual Entry",
            category,
            quantity,
            price,
            "INR",
            "MANUAL_ADD"
        ]
        
        with open(csv_file, 'a', newline='') as f:
            writer = import_csv.writer(f)
            # if not file_exists: writer.writerow(...) # Assuming existing file
            writer.writerow(new_row)
            
        # 2. Update SQLite
        import sqlite3
        import datetime
        
        conn = sqlite3.connect('enterprise_data.db')
        cursor = conn.cursor()
        
        # Upsert logic (Replace or Insert)
        cursor.execute("""
            INSERT INTO master_data (product_code, description, price, quantity, final_amount, last_updated_at)
            VALUES (?, ?, ?, ?, 0, ?)
            ON CONFLICT(product_code) DO UPDATE SET
                description = excluded.description,
                price = excluded.price,
                quantity = excluded.quantity,
                last_updated_at = excluded.last_updated_at
        """, (p_code, category, float(price), int(quantity), datetime.datetime.now()))
        
        conn.commit()
        conn.close()
        
        flash(f"Success: Product {p_code} added/updated!")
        return redirect(url_for('index'))
        
    except Exception as e:
        flash(f"System Error: {str(e)}")
        return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
