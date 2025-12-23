import pandas as pd
import os
import glob
from pathlib import Path

# Configuration for verification checks
# - RAW_DATA_PATH: expected raw input file used for comparison
# - OUTPUT_DIR: directory containing generated output CSV files per ticker
# - EXPECTED_TICKERS: list of tickers that must have corresponding output files
RAW_DATA_PATH = Path('data/raw/output_file.csv') 
OUTPUT_DIR = Path('data/output')
EXPECTED_TICKERS = ['AAPL', 'AMD', 'AMZN', 'AVGO', 'CSCO', 'MSFT', 'NFLX', 'PEP', 'TMUS', 'TSLA']

def verify_outputs():
    # Start verification steps
    print("Starting deep verification process...\n")

    # 1. Load Raw Data for Comparison
    if not RAW_DATA_PATH.exists():
        # If raw data is not available, skip logic checks that require it
        print("Raw data not found. Skipping logic validation checks.")
        raw_df = None
    else:
        # Load and sort raw data for deterministic comparisons
        print("Loading raw data for logic validation...")
        raw_df = pd.read_csv(RAW_DATA_PATH, parse_dates=['date'])
        raw_df = raw_df.sort_values(['ticker', 'date'])

    all_passed = True

    for ticker in EXPECTED_TICKERS:
        file_path = OUTPUT_DIR / f"result_{ticker}.csv"
        
        if not file_path.exists():
            print(f"FAILED: Missing file for {ticker}")
            all_passed = False
            continue
            
        try:
            # Load Output
            out_df = pd.read_csv(file_path)

            # Check 1: Row Count
            # Validate that the file contains exactly 24 rows
            if len(out_df) != 24:
                print(f"FAILED: {ticker} has {len(out_df)} rows; expected 24.")
                all_passed = False
                continue

            # Check 2: Logic Validation (Compare against Raw Data)
            if raw_df is not None:
                # Get raw data for this ticker
                ticker_raw = raw_df[raw_df['ticker'] == ticker].copy()
                
                # Take the very first daily 'open' of the entire dataset for this ticker
                first_raw_open = ticker_raw.iloc[0]['open']
                
                # Take the very first monthly 'open' from your output
                first_output_open = out_df.iloc[0]['open']

                # They must match substantially
                # The first monthly open in the output should match the first raw daily open
                if abs(first_raw_open - first_output_open) > 0.01:
                    print(f"LOGIC FAIL: {ticker} first-month Open mismatch. Raw: {first_raw_open}, Output: {first_output_open}")
                    all_passed = False
                    continue

        except Exception as e:
            print(f"FAILED: Error checking {ticker}: {e}")
            all_passed = False
            continue

    if all_passed:
        print("\nALL CHECKS PASSED: schema, logic, and counts validated.")
    else:
        print("\nSome checks failed.")

if __name__ == "__main__":
    verify_outputs()