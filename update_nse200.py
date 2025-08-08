#!/usr/bin/env python3
"""
NSE 200 List Updater

This script fetches the latest NSE 200 constituent list and updates the Excel file
with Upstox instrument keys for use in the trading algorithm.
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import argparse
from pathlib import Path

# Configuration
NSE_NIFTY200_CSV_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv"
NSE_NIFTY200_BACKUP_URL = "https://www1.nseindia.com/content/indices/ind_nifty200list.csv"
UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
OUTPUT_FILE = "ind_nifty200list.xlsx"
BACKUP_DIR = "backups"

class NSE200Updater:
    def __init__(self):
        self.session = requests.Session()
        # Set headers to mimic browser request
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nseindia.com/',
            'X-Requested-With': 'XMLHttpRequest'
        })
    
    def get_nse_session(self) -> bool:
        """Get NSE session by visiting the main page first"""
        try:
            print("Establishing NSE session...")
            response = self.session.get("https://www.nseindia.com/", timeout=30)
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to establish NSE session: {e}")
            return False
    
    def fetch_nse200_list(self) -> Optional[List[Dict]]:
        """Fetch NSE 200 constituents from NSE CSV"""
        print("Fetching NSE 200 constituents from CSV...")
        
        urls_to_try = [NSE_NIFTY200_CSV_URL, NSE_NIFTY200_BACKUP_URL]
        
        for url in urls_to_try:
            try:
                print(f"Trying: {url}")
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    # Parse CSV content
                    import io
                    df = pd.read_csv(io.StringIO(response.text))
                    
                    # Convert to list of dictionaries
                    stocks = []
                    for _, row in df.iterrows():
                        stock = {
                            'Company Name': str(row.get('Company Name', '')).strip(),
                            'Industry': str(row.get('Industry', '')).strip(),
                            'Symbol': str(row.get('Symbol', '')).strip(),
                            'Series': str(row.get('Series', 'EQ')).strip(),
                            'ISIN Code': str(row.get('ISIN Code', '')).strip()
                        }
                        if stock['Symbol'] and stock['Symbol'] != 'nan':
                            stocks.append(stock)
                    
                    print(f"Retrieved {len(stocks)} stocks from NSE CSV")
                    if len(stocks) >= 190:  # Should be around 200
                        return stocks
                    else:
                        print(f"Warning: Only found {len(stocks)} stocks, expected ~200")
                        return stocks
                        
                else:
                    print(f"HTTP {response.status_code} from {url}")
                    
            except Exception as e:
                print(f"Error fetching from {url}: {e}")
                continue
        
        print("Failed to fetch from all NSE URLs")
        return None
    
    def fetch_upstox_instruments(self) -> Optional[pd.DataFrame]:
        """Fetch Upstox instrument master list"""
        try:
            print("Fetching Upstox instrument master...")
            
            # Download the compressed CSV
            response = requests.get(UPSTOX_INSTRUMENTS_URL, timeout=60)
            if response.status_code != 200:
                print(f"Upstox API returned status code: {response.status_code}")
                return None
            
            # The response is gzipped CSV content
            import gzip
            import io
            
            # Decompress the content
            decompressed = gzip.decompress(response.content)
            csv_content = decompressed.decode('utf-8')
            
            # Parse CSV
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Filter for NSE equity instruments only
            nse_eq = df[
                (df['exchange'] == 'NSE') & 
                (df['instrument_type'] == 'EQ') & 
                (df['name'].str.len() > 0)
            ].copy()
            
            print(f"Found {len(nse_eq)} NSE equity instruments")
            return nse_eq
            
        except Exception as e:
            print(f"Error fetching Upstox instruments: {e}")
            return None
    
    def match_instruments(self, nse_stocks: List[Dict], upstox_df: pd.DataFrame) -> pd.DataFrame:
        """Match NSE stocks with Upstox instrument keys"""
        print("Matching NSE stocks with Upstox instruments...")
        
        # Create lookup dictionary for faster matching
        upstox_lookup = {}
        for _, row in upstox_df.iterrows():
            symbol = row['tradingsymbol']
            upstox_lookup[symbol] = {
                'instrument_key': row['instrument_key'],
                'isin': row.get('isin', ''),
                'company_name': row.get('name', '')
            }
        
        matched_stocks = []
        unmatched_stocks = []
        
        for stock in nse_stocks:
            symbol = stock['Symbol']
            
            if symbol in upstox_lookup:
                matched_stock = stock.copy()
                matched_stock.update(upstox_lookup[symbol])
                matched_stock['Series'] = 'EQ'  # All are equity series
                matched_stock['ISIN Code'] = matched_stock['isin']
                matched_stocks.append(matched_stock)
            else:
                unmatched_stocks.append(symbol)
        
        print(f"Matched: {len(matched_stocks)} stocks")
        if unmatched_stocks:
            print(f"Unmatched: {len(unmatched_stocks)} stocks: {', '.join(unmatched_stocks[:10])}{'...' if len(unmatched_stocks) > 10 else ''}")
        
        # Convert to DataFrame with proper column order
        if matched_stocks:
            df = pd.DataFrame(matched_stocks)
            column_order = ['Company Name', 'Industry', 'Symbol', 'Series', 'ISIN Code', 'instrument_key']
            # Only include columns that exist
            available_columns = [col for col in column_order if col in df.columns]
            df = df[available_columns]
            return df
        else:
            return pd.DataFrame()
    
    def backup_current_file(self) -> bool:
        """Backup the current NSE 200 file"""
        current_file = Path(OUTPUT_FILE)
        if not current_file.exists():
            print("No existing file to backup")
            return True
        
        try:
            # Create backup directory
            backup_path = Path(BACKUP_DIR)
            backup_path.mkdir(exist_ok=True)
            
            # Create backup with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_path / f"ind_nifty200list_backup_{timestamp}.xlsx"
            
            # Copy file
            import shutil
            shutil.copy2(current_file, backup_file)
            
            print(f"Backup created: {backup_file}")
            return True
            
        except Exception as e:
            print(f"Failed to create backup: {e}")
            return False
    
    def save_updated_list(self, df: pd.DataFrame) -> bool:
        """Save the updated NSE 200 list"""
        try:
            df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
            print(f"Updated NSE 200 list saved to: {OUTPUT_FILE}")
            print(f"Total stocks: {len(df)}")
            return True
        except Exception as e:
            print(f"Failed to save file: {e}")
            return False
    
    def compare_with_existing(self, new_df: pd.DataFrame) -> None:
        """Compare new list with existing file"""
        current_file = Path(OUTPUT_FILE)
        if not current_file.exists():
            print("No existing file to compare with")
            return
        
        try:
            old_df = pd.read_excel(current_file)
            old_symbols = set(old_df['Symbol'].tolist()) if 'Symbol' in old_df.columns else set()
            new_symbols = set(new_df['Symbol'].tolist()) if 'Symbol' in new_df.columns else set()
            
            added = new_symbols - old_symbols
            removed = old_symbols - new_symbols
            
            print("\nComparison with existing file:")
            print(f"Previous count: {len(old_symbols)}")
            print(f"New count: {len(new_symbols)}")
            
            if added:
                print(f"Added symbols ({len(added)}): {', '.join(sorted(added))}")
            
            if removed:
                print(f"Removed symbols ({len(removed)}): {', '.join(sorted(removed))}")
            
            if not added and not removed:
                print("No changes in stock list")
            
        except Exception as e:
            print(f"Error comparing files: {e}")
    
    def update(self, force: bool = False) -> bool:
        """Main update process"""
        print("NSE 200 List Updater")
        print("=" * 50)
        
        # Fetch NSE 200 list
        nse_stocks = self.fetch_nse200_list()
        if not nse_stocks:
            print("Failed to fetch NSE 200 list")
            return False
        
        # Fetch Upstox instruments
        upstox_df = self.fetch_upstox_instruments()
        if upstox_df is None:
            print("Failed to fetch Upstox instruments")
            return False
        
        # Match instruments
        matched_df = self.match_instruments(nse_stocks, upstox_df)
        if matched_df.empty:
            print("No instruments were matched")
            return False
        
        # Check if we have reasonable coverage
        match_percentage = (len(matched_df) / len(nse_stocks)) * 100
        print(f"Match coverage: {match_percentage:.1f}%")
        
        if match_percentage < 90 and not force:
            print("Low match coverage. Use --force to proceed anyway.")
            return False
        
        # Compare with existing
        self.compare_with_existing(matched_df)
        
        # Backup existing file
        if not self.backup_current_file():
            print("Backup failed. Use --force to proceed anyway.")
            if not force:
                return False
        
        # Save updated list
        return self.save_updated_list(matched_df)


def main():
    parser = argparse.ArgumentParser(description="Update NSE 200 constituent list")
    parser.add_argument('--force', action='store_true', help='Force update even if coverage is low')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without saving')
    
    args = parser.parse_args()
    
    updater = NSE200Updater()
    
    if args.dry_run:
        print("DRY RUN MODE - No files will be modified")
        print()
        
        # Run the update process but don't save files
        print("NSE 200 List Updater (DRY RUN)")
        print("=" * 50)
        
        # Fetch NSE 200 list
        nse_stocks = updater.fetch_nse200_list()
        if not nse_stocks:
            print("Failed to fetch NSE 200 list")
            return 1
        
        # Fetch Upstox instruments
        upstox_df = updater.fetch_upstox_instruments()
        if upstox_df is None:
            print("Failed to fetch Upstox instruments")
            return 1
        
        # Match instruments
        matched_df = updater.match_instruments(nse_stocks, upstox_df)
        if matched_df.empty:
            print("No instruments were matched")
            return 1
        
        # Show results
        match_percentage = (len(matched_df) / len(nse_stocks)) * 100
        print(f"Match coverage: {match_percentage:.1f}%")
        
        # Compare with existing
        updater.compare_with_existing(matched_df)
        
        print(f"\nDRY RUN: Would save {len(matched_df)} stocks to {OUTPUT_FILE}")
        print("No files were modified.")
        return 0
    
    success = updater.update(force=args.force)
    
    if success:
        print("\n✓ NSE 200 list updated successfully!")
    else:
        print("\n✗ Failed to update NSE 200 list")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())