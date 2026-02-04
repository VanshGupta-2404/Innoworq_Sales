import os
import glob
import subprocess
import sys

UPLOAD_DIR = 'uploads'
RECONCILE_SCRIPT = 'secure_reconcile.py'

def main():
    if not os.path.exists(UPLOAD_DIR):
        print(f"Error: Directory '{UPLOAD_DIR}' does not exist.")
        return

    # Find all files
    files = glob.glob(os.path.join(UPLOAD_DIR, '*'))
    files = [f for f in files if os.path.isfile(f) and not f.startswith('.')]
    
    if not files:
        print(f"No files found in '{UPLOAD_DIR}'.")
        print("Please place your Excel/CSV file there and run this script again.")
        return

    # Sort by modification time (newest first)
    files.sort(key=os.path.getmtime, reverse=True)
    
    latest_file = files[0]
    print(f"Found {len(files)} file(s).")
    print(f"Processing latest file: {latest_file}")
    
    # Run the reconciliation script
    cmd = [sys.executable, RECONCILE_SCRIPT, latest_file]
    subprocess.run(cmd)

if __name__ == "__main__":
    main()
