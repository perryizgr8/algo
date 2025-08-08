#!/usr/bin/env python3
"""
Simple NSE 200 List Updater

This script helps update the NSE 200 list by:
1. Showing how to manually download the latest list from NSE
2. Matching any new CSV with Upstox instrument keys
3. Updating the Excel file used by the algorithm

Since NSE has anti-scraping measures, this approach uses manual download
with automatic Upstox instrument key matching.
"""

import pandas as pd
import requests
import argparse
from pathlib import Path
from datetime import datetime
import gzip
import io

# Configuration
UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
OUTPUT_FILE = "ind_nifty200list.xlsx"
BACKUP_DIR = "backups"


def fetch_upstox_instruments():
    """Fetch Upstox instrument master list"""
    print("Fetching Upstox instrument master...")
    
    try:
        response = requests.get(UPSTOX_INSTRUMENTS_URL, timeout=60)
        if response.status_code != 200:
            print(f"Upstox API returned status code: {response.status_code}")
            return None
        
        # Decompress the gzipped CSV
        decompressed = gzip.decompress(response.content)
        csv_content = decompressed.decode('utf-8')
        
        # Parse CSV
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Filter for NSE equity instruments only
        nse_eq = df[
            (df['exchange'] == 'NSE_EQ') & 
            (df['instrument_type'] == 'EQUITY') & 
            (df['name'].str.len() > 0)
        ].copy()
        
        print(f"Found {len(nse_eq)} NSE equity instruments in Upstox master")
        return nse_eq
        
    except Exception as e:
        print(f"Error fetching Upstox instruments: {e}")
        return None


def match_with_upstox(input_file, upstox_df):
    """Match input CSV/Excel with Upstox instruments"""
    print(f"Processing input file: {input_file}")
    
    # Read input file
    try:
        if input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        elif input_file.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file)
        else:
            print("Unsupported file format. Use CSV or Excel file.")
            return None
    except Exception as e:
        print(f"Error reading input file: {e}")
        return None
    
    print(f"Input file has {len(df)} rows")
    
    # Look for symbol column (try different possible names)
    symbol_col = None
    for col in ['Symbol', 'SYMBOL', 'symbol', 'Stock Symbol', 'Ticker']:
        if col in df.columns:
            symbol_col = col
            break
    
    if symbol_col is None:
        print("Could not find symbol column. Available columns:")
        print(df.columns.tolist())
        return None
    
    print(f"Using '{symbol_col}' as symbol column")
    
    # Create lookup dictionary for Upstox instruments
    upstox_lookup = {}
    for _, row in upstox_df.iterrows():
        symbol = row['tradingsymbol']
        upstox_lookup[symbol] = {
            'instrument_key': row['instrument_key'],
            'isin': row.get('isin', ''),
            'upstox_name': row.get('name', '')
        }
    
    # Match symbols
    matched_count = 0
    unmatched_symbols = []
    
    # Add instrument key column
    df['instrument_key'] = ''
    df['ISIN Code'] = ''
    
    for idx, row in df.iterrows():
        symbol = str(row[symbol_col]).strip()
        
        if symbol in upstox_lookup:
            df.at[idx, 'instrument_key'] = upstox_lookup[symbol]['instrument_key']
            df.at[idx, 'ISIN Code'] = upstox_lookup[symbol]['isin']
            matched_count += 1
        else:
            unmatched_symbols.append(symbol)
    
    print(f"Matched: {matched_count}/{len(df)} symbols ({matched_count/len(df)*100:.1f}%)")
    
    if unmatched_symbols:
        print(f"Unmatched symbols ({len(unmatched_symbols)}):")
        for symbol in unmatched_symbols[:10]:  # Show first 10
            print(f"  - {symbol}")
        if len(unmatched_symbols) > 10:
            print(f"  ... and {len(unmatched_symbols) - 10} more")
    
    return df


def backup_current_file():
    """Backup the current NSE 200 file"""
    current_file = Path(OUTPUT_FILE)
    if not current_file.exists():
        print("No existing file to backup")
        return True
    
    try:
        backup_path = Path(BACKUP_DIR)
        backup_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_path / f"ind_nifty200list_backup_{timestamp}.xlsx"
        
        import shutil
        shutil.copy2(current_file, backup_file)
        
        print(f"Backup created: {backup_file}")
        return True
        
    except Exception as e:
        print(f"Failed to create backup: {e}")
        return False


def save_updated_list(df):
    """Save the updated NSE 200 list"""
    try:
        df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
        print(f"Updated NSE 200 list saved to: {OUTPUT_FILE}")
        print(f"Total stocks: {len(df)}")
        return True
    except Exception as e:
        print(f"Failed to save file: {e}")
        return False


def print_instructions():
    """Print manual download instructions"""
    print("\nMANUAL DOWNLOAD INSTRUCTIONS:")
    print("=" * 50)
    print("1. Visit: https://www.nseindia.com/products-services/indices-nifty200-index")
    print("2. Look for 'Download' or 'Constituents' section")
    print("3. Download the CSV or Excel file with NSE 200 constituents")
    print("4. Save it in this directory")
    print("5. Run this script with: --input <downloaded_file>")
    print("\nAlternative sources:")
    print("- NSE website data section")
    print("- Financial data providers (Yahoo Finance, etc.)")
    print("- Manual copy-paste from NSE website into Excel")


def main():
    parser = argparse.ArgumentParser(
        description="Simple NSE 200 List Updater with manual download",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python update_nse200_simple.py --instructions
  python update_nse200_simple.py --input downloaded_nse200.csv
  python update_nse200_simple.py --input nse_data.xlsx
        """
    )
    
    parser.add_argument('--input', help='Input CSV or Excel file with NSE 200 data')
    parser.add_argument('--instructions', action='store_true', help='Show download instructions')
    parser.add_argument('--test-upstox', action='store_true', help='Test Upstox connection only')
    
    args = parser.parse_args()
    
    if args.instructions:
        print_instructions()
        return 0
    
    if args.test_upstox:
        print("Testing Upstox instruments download...")
        upstox_df = fetch_upstox_instruments()
        if upstox_df is not None:
            print("SUCCESS: Upstox connection working")
            print(f"Found {len(upstox_df)} NSE equity instruments")
            
            # Show a few examples
            sample = upstox_df.head(3)
            print("\nSample instruments:")
            for _, row in sample.iterrows():
                print(f"  {row['tradingsymbol']} -> {row['instrument_key']}")
        else:
            print("ERROR: Upstox connection failed")
            return 1
        return 0
    
    if not args.input:
        print("Please provide an input file or use --instructions")
        print("Usage: python update_nse200_simple.py --input <file>")
        return 1
    
    input_file = args.input
    if not Path(input_file).exists():
        print(f"Input file not found: {input_file}")
        return 1
    
    # Fetch Upstox instruments
    upstox_df = fetch_upstox_instruments()
    if upstox_df is None:
        print("Failed to fetch Upstox instruments")
        return 1
    
    # Match with input file
    matched_df = match_with_upstox(input_file, upstox_df)
    if matched_df is None:
        print("Failed to process input file")
        return 1
    
    # Show results
    instrument_key_count = matched_df['instrument_key'].ne('').sum()
    match_percentage = (instrument_key_count / len(matched_df)) * 100
    
    print(f"\nResults:")
    print(f"Total stocks: {len(matched_df)}")
    print(f"With instrument keys: {instrument_key_count}")
    print(f"Match rate: {match_percentage:.1f}%")
    
    if match_percentage < 90:
        print("Warning: Low match rate. Check if symbols are correct.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return 1
    
    # Backup and save
    if not backup_current_file():
        print("Backup failed")
        return 1
    
    if save_updated_list(matched_df):
        print("\nSUCCESS: NSE 200 list updated successfully!")
        return 0
    else:
        print("\nERROR: Failed to save updated list")
        return 1


if __name__ == "__main__":
    exit(main())