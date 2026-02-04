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
        # Create a temporary test database
        self.test_db = 'test_price_db.csv'
        self.test_log_dir = 'test_logs'
        self.test_log_file = os.path.join(self.test_log_dir, 'admin_actions.log')
        
        df = pd.DataFrame({
            'Product_ID': [1, 2],
            'Product_Name': ['TestItem1', 'TestItem2'],
            'Unit_Price': [10.0, 20.0]
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
        # Create a dummy user file
        user_file = 'user_upload_test.csv'
        pd.DataFrame({'Product_Name': ['TestItem1'], 'Quantity': [5]}).to_csv(user_file, index=False)
        
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
        
        # Check if Unit_Price is NOT in output
        self.assertNotIn("Unit_Price", output)
        self.assertIn("Item_Total", output)
        self.assertIn("Grand Total", output)
        
        # calculate expected total: 10.0 * 5 = 50.0
        self.assertIn("50.00", output)

    @patch('builtins.input', side_effect=['99', 'NewItem', '50.0'])
    def test_admin_add_product(self, mock_input):
        secure_processor.add_product()
        df = pd.read_csv(self.test_db)
        self.assertTrue(99 in df['Product_ID'].values)
        self.assertTrue('NewItem' in df['Product_Name'].values)

    @patch('builtins.input', return_value='1')
    @patch('getpass.getpass', return_value='wrongpassword')
    def test_admin_delete_fail_wrong_password(self, mock_getpass, mock_input):
        # Note: arguments are passed bottom-up -> mock_getpass (inner), mock_input (outer)?
        # Actually in python 3 it is Top-down?
        # Let's just create the mocks and not worry about arg order if we don't use them directly (except mock_getpass return value)
        # But we do use side_effects above.
        # Python docs say: @patch('A'), @patch('B') -> def test(mock_A, mock_B)
        
        secure_processor.delete_product()
        df = pd.read_csv(self.test_db)
        self.assertTrue(1 in df['Product_ID'].values) # Should NOT be deleted

    @patch('builtins.input', return_value='1')
    @patch('getpass.getpass', return_value='admin123')
    def test_admin_delete_success(self, mock_getpass, mock_input):
        secure_processor.delete_product()
        df = pd.read_csv(self.test_db)
        self.assertFalse(1 in df['Product_ID'].values) # Should be deleted

if __name__ == '__main__':
    unittest.main()
