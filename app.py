from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import os
import uuid
import sqlite3
import datetime
import pandas as pd
from secure_reconcile import ReconciliationEngine

app = Flask(__name__)
app.secret_key = "super_secure_secret_key"
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
RESULTS_FOLDER = os.path.join(BASE_DIR, "results")
DB_PATH = os.path.join(BASE_DIR, "enterprise_data.db")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        flash("No file uploaded")
        return redirect(url_for("index"))

    file = request.files["file"]
    if not file.filename:
        flash("No file selected")
        return redirect(url_for("index"))

    temp_name = f"TEMP_{uuid.uuid4()}_{file.filename}"
    temp_path = os.path.join(UPLOAD_FOLDER, temp_name)
    file.save(temp_path)

    try:
        engine = ReconciliationEngine()  # ✅ lazy init
        enriched_df, summary = engine.process_file(temp_path)

        if enriched_df is None:
            flash(summary.get("error", "Processing failed"))
            return redirect(url_for("index"))

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = f"reconciled_{timestamp}.xlsx"
        out_path = os.path.join(RESULTS_FOLDER, out_file)

        enriched_df.to_excel(out_path, index=False)
        return send_file(out_path, as_attachment=True)

    except Exception as e:
        flash(str(e))
        return redirect(url_for("index"))

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

@app.route("/add_product", methods=["POST"])
def add_product():
    p_code = request.form.get("product_code")
    category = request.form.get("category")
    price = request.form.get("price")
    quantity = request.form.get("quantity")

    if not all([p_code, category, price, quantity]):
        flash("All fields required")
        return redirect(url_for("index"))

    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO master_data (product_code, description, price, quantity, final_amount, last_updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_code) DO UPDATE SET
            description=excluded.description,
            price=excluded.price,
            quantity=excluded.quantity,
            final_amount=excluded.price * excluded.quantity,
            last_updated_at=excluded.last_updated_at
    """, (
        p_code,
        category,
        float(price),
        int(quantity),
        float(price) * int(quantity),
        datetime.datetime.now()
    ))

    conn.commit()
    conn.close()

    flash("Product saved successfully")
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run()
