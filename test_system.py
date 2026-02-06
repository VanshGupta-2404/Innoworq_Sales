import unittest
import pandas as pd
import os
import sys
from unittest.mock import patch
import io

# Modify sys.path to ensure we can import secure_processor
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import secure_processor

class TestSecureSystem(unittest.TestCase):
    
    def setUp(self):
        # Create a temporary test database with NEW schema
        self.test_db = 'test_price_db.csv'
        self.test_log_dir = 'test_logs'
        self.test_log_file = os.path.join(self.test_log_dir, 'admin_actions.log')
        
        # New Schema: model, price
        df = pd.DataFrame({
            'model': ['Item1', 'Item2'],
            'price': [10.0, 20.0]
        })
        df.to_csv(self.test_db, index=False)
        
        # Patch the configuration in secure_processor
        secure_processor.PRICE_DB_FILE = self.test_db
        # Re-setup logging to test directory
        secure_processor.setup_logging(self.test_log_file)

    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        if os.path.exists(self.test_log_dir):
            import shutil
            shutil.rmtree(self.test_log_dir)

    @patch('builtins.input')
    def test_standard_user_view_hides_price(self, mock_input):
        # User upload uses 'Product_Name' which maps to 'model'
        user_file = 'user_upload_test.csv'
        pd.DataFrame({'Product_Name': ['Item1'], 'Quantity': [5]}).to_csv(user_file, index=False)
        
        mock_input.return_value = user_file
        
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured_output
        
        try:
            secure_processor.standard_user_flow()
        finally:
            sys.stdout = old_stdout
            if os.path.exists(user_file):
                os.remove(user_file)
        
        output = captured_output.getvalue()
        
        # Check if output contains Item Total (10.0 * 5 = 50.0)
        self.assertIn("Item_Total", output)
        self.assertIn("50.00", output)

    @patch('builtins.input', side_effect=['NewModel', 'New Description', '100.0']) 
    def test_admin_add_product(self, mock_input):
        # Input: ID(Model), Name(Desc), Price
        secure_processor.add_product()
        df = pd.read_csv(self.test_db)
        # Check if NewModel is in 'model' column
        self.assertTrue('NewModel' in df['model'].values)

    @patch('builtins.input', return_value='Item1')
    @patch('getpass.getpass', return_value='wrongpassword')
    def test_admin_delete_fail_wrong_password(self, mock_getpass, mock_input):
        secure_processor.delete_product()
        df = pd.read_csv(self.test_db)
        self.assertTrue('Item1' in df['model'].values) # Should NOT be deleted

    @patch('builtins.input', return_value='Item1')
    @patch('getpass.getpass', return_value='admin123')
    def test_admin_delete_success(self, mock_getpass, mock_input):
        secure_processor.delete_product()
        df = pd.read_csv(self.test_db)
        self.assertFalse('Item1' in df['model'].values) # Should be deleted

if __name__ == '__main__':
    unittest.main()
