import unittest
import pandas as pd
from src.transformer import resample_to_monthly, calculate_technical_indicators

class TestFinancialLogic(unittest.TestCase):
    
    def setUp(self):
        # Create synthetic daily data: 2 months of data for 1 ticker
        dates = pd.date_range(start='2023-01-01', end='2023-02-28', freq='D')
        self.df = pd.DataFrame({
            'date': dates,
            'ticker': 'TEST',
            'open': 100.0,
            'high': 110.0,
            'low': 90.0,
            'close': 105.0, # Constant close makes SMA easy to check
            'volume': 1000
        })
        # Modify specific days to test Open/Close snapshot logic
        # Jan 1st Open = 100 (Default)
        # Jan 31st Close = 200
        self.df.loc[self.df['date'] == '2023-01-31', 'close'] = 200.0
        
        # Feb 1st Open = 150
        self.df.loc[self.df['date'] == '2023-02-01', 'open'] = 150.0

    def test_ohlc_aggregation(self):
        """Test if Open is first day and Close is last day."""
        monthly = resample_to_monthly(self.df)
        
        # We expect 2 rows (Jan and Feb)
        self.assertEqual(len(monthly), 2)
        
        # Check Jan Logic
        jan_row = monthly.iloc[0]
        self.assertEqual(jan_row['open'], 100.0) # First day open
        self.assertEqual(jan_row['close'], 200.0) # Last day close (snapshot)

        # Check Feb Logic
        feb_row = monthly.iloc[1]
        self.assertEqual(feb_row['open'], 150.0) # First day open

    def test_indicator_shapes(self):
        """Test if indicators are added."""
        monthly = resample_to_monthly(self.df)
        final = calculate_technical_indicators(monthly)
        
        self.assertIn('sma_10', final.columns)
        self.assertIn('ema_20', final.columns)

if __name__ == '__main__':
    unittest.main()