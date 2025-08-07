"""
Utility functions for NSE 200 winner algorithm
"""

import pandas as pd
import requests
from datetime import datetime
import time
from typing import List, Dict, Optional, Tuple
from config import get_api_headers, UPSTOX_BASE_URL, NSE200_FILE
from cache import api_cache


def datesort(crow):
    """Sort function for date strings in candle data"""
    dt = crow[0]
    return int(datetime.fromisoformat(dt).timestamp())


def get_returns(instkey: str, weeks: int, max_retries: int = 3) -> Optional[float]:
    """
    Get returns for a stock over specified number of weeks
    
    Args:
        instkey: Upstox instrument key
        weeks: Number of weeks to calculate returns for
        max_retries: Maximum number of API retries
    
    Returns:
        Returns as decimal (e.g., 0.15 for 15%) or None if failed
    """
    from config import get_date_range
    
    start_date, end_date = get_date_range(weeks)
    url = f"{UPSTOX_BASE_URL}/historical-candle/{instkey}/month/{end_date}/{start_date}"
    
    # Create cache key parameters
    cache_params = {
        'instkey': instkey,
        'start_date': start_date,
        'end_date': end_date,
        'interval': 'month'
    }
    
    # Try to get from cache first
    cached_response = api_cache.get(url, cache_params)
    if cached_response is not None:
        # Process cached data
        return _process_candle_data(cached_response, instkey)
    
    # Not in cache, make API request
    headers = get_api_headers()
    
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code == 429:  # Rate limit
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
                
            if resp.status_code != 200:
                print(f"API error for {instkey}: {resp.status_code} - {resp.text}")
                return None
                
            rjson = resp.json()
            
            if rjson.get("status") != "success":
                print(f"API status error for {instkey}: {rjson.get('status')}")
                return None
            
            # Cache the successful response
            api_cache.set(url, rjson, cache_params)
            
            # Process the response
            return _process_candle_data(rjson, instkey)
            
        except requests.exceptions.RequestException as e:
            print(f"Request error for {instkey}: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"Unexpected error for {instkey}: {e}")
            return None
    
    return None


def _process_candle_data(response_data: Dict, instkey: str) -> Optional[float]:
    """
    Process candle data from API response to calculate returns
    
    Args:
        response_data: The API response JSON
        instkey: Upstox instrument key (for error reporting)
    
    Returns:
        Returns as decimal or None if failed
    """
    candles = response_data.get("data", {}).get("candles", [])
    if not candles:
        print(f"No candle data for {instkey}")
        return None
        
    candles.sort(key=datesort, reverse=True)
    
    start_price = candles[-1][4]  # Closing price of earliest date
    end_price = candles[0][4]     # Closing price of latest date
    
    if start_price == 0:
        return None
        
    return (end_price - start_price) / start_price


def load_nse200_data() -> pd.DataFrame:
    """Load NSE 200 stock data from Excel file"""
    try:
        return pd.read_excel(NSE200_FILE)
    except FileNotFoundError:
        print(f"Error: {NSE200_FILE} not found")
        raise
    except Exception as e:
        print(f"Error loading NSE200 data: {e}")
        raise


def calculate_returns_for_all_stocks(weeks: int) -> List[Dict[str, any]]:
    """
    Calculate returns for all NSE 200 stocks
    
    Args:
        weeks: Number of weeks for return calculation
        
    Returns:
        List of dicts with symbol and gain/return data
    """
    df = load_nse200_data()
    sym_returns = {}
    total_stocks = len(df)
    
    print(f"Calculating {weeks}-week returns for {total_stocks} stocks...")
    
    for idx, row in df.iterrows():
        symbol = row['Symbol']
        instkey = row['instrument_key']
        
        print(f"Processing {idx+1}/{total_stocks}: {symbol}")
        
        returns = get_returns(instkey, weeks)
        sym_returns[symbol] = returns if returns is not None else 0
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)
    
    # Sort by returns (highest first)
    sorted_returns = []
    for symbol, gain in sorted(sym_returns.items(), key=lambda x: x[1], reverse=True):
        sorted_returns.append({"symbol": symbol, "gain": gain})
    
    return sorted_returns


def load_current_portfolio(portfolio_file: str) -> pd.DataFrame:
    """Load current portfolio from CSV file"""
    try:
        return pd.read_csv(portfolio_file)
    except FileNotFoundError:
        print(f"Portfolio file {portfolio_file} not found, creating empty portfolio")
        df = pd.DataFrame(columns=['Symbol', 'Units'])
        df.to_csv(portfolio_file, index=False)
        return df
    except Exception as e:
        print(f"Error loading portfolio: {e}")
        raise


def calculate_portfolio_changes(current_portfolio: pd.DataFrame, 
                              top_40: List[Dict], 
                              top_20: List[Dict]) -> Tuple[List[str], List[str], List[str]]:
    """
    Calculate buy, sell, and hold decisions
    
    Args:
        current_portfolio: Current portfolio DataFrame
        top_40: Top 40 performing stocks
        top_20: Top 20 performing stocks
        
    Returns:
        Tuple of (buy_list, sell_list, hold_list)
    """
    sell = []
    buy = []
    hold = []
    
    # Convert to sets for faster lookups
    top_40_symbols = {stock['symbol'] for stock in top_40}
    top_20_symbols = {stock['symbol'] for stock in top_20}
    current_symbols = set(current_portfolio['Symbol'].values)
    
    # Determine sells and holds from current portfolio
    for symbol in current_symbols:
        if symbol not in top_40_symbols:
            sell.append(symbol)
        elif symbol in top_20_symbols:
            # Already in top 20, will be in buy list
            pass
        else:
            # In top 40 but not top 20
            hold.append(symbol)
    
    # Add all top 20 to buy list (including ones we already hold)
    buy = [stock['symbol'] for stock in top_20]
    
    return buy, sell, hold


def update_portfolio(portfolio_file: str, buy_list: List[str], sell_list: List[str]) -> None:
    """
    Update portfolio CSV file with buy and sell decisions
    
    Args:
        portfolio_file: Path to portfolio CSV file
        buy_list: List of symbols to buy
        sell_list: List of symbols to sell
    """
    df = load_current_portfolio(portfolio_file)
    
    # Remove sold symbols
    for symbol in sell_list:
        df = df[df['Symbol'] != symbol]
    
    # Add new symbols to buy
    current_symbols = set(df['Symbol'].values)
    for symbol in buy_list:
        if symbol not in current_symbols:
            new_row = pd.DataFrame({'Symbol': [symbol], 'Units': [0]})
            df = pd.concat([df, new_row], ignore_index=True)
    
    # Save updated portfolio
    df.to_csv(portfolio_file, index=False)
    print(f"Portfolio updated: {len(buy_list)} buys, {len(sell_list)} sells")


def print_portfolio_summary(buy_list: List[str], sell_list: List[str], hold_list: List[str]) -> None:
    """Print a summary of portfolio changes"""
    print("\n" + "="*50)
    print("PORTFOLIO REBALANCING SUMMARY")
    print("="*50)
    
    if buy_list:
        print(f"\nBUY ({len(buy_list)} stocks):")
        for i, symbol in enumerate(buy_list, 1):
            print(f"  {i:2d}. {symbol}")
    
    if sell_list:
        print(f"\nSELL ({len(sell_list)} stocks):")
        for i, symbol in enumerate(sell_list, 1):
            print(f"  {i:2d}. {symbol}")
    
    if hold_list:
        print(f"\nHOLD ({len(hold_list)} stocks):")
        for i, symbol in enumerate(hold_list, 1):
            print(f"  {i:2d}. {symbol}")
    
    if not sell_list and not buy_list:
        print("\nNo changes needed - portfolio already optimal")
    
    print("="*50)


def print_top_performers(sorted_stocks: List[Dict], top_n: int = 20) -> None:
    """Print top N performing stocks"""
    print(f"\nTOP {top_n} PERFORMERS:")
    print("-" * 40)
    for i, stock in enumerate(sorted_stocks[:top_n], 1):
        gain_pct = stock['gain'] * 100
        print(f"{i:2d}. {stock['symbol']:12s} {gain_pct:+7.2f}%")


def clear_api_cache() -> int:
    """
    Clear all cached API data
    
    Returns:
        Number of cache files removed
    """
    from cache import api_cache
    return api_cache.clear_all()


def print_cache_stats() -> None:
    """Print cache statistics"""
    from cache import api_cache
    stats = api_cache.get_cache_stats()
    
    print("\nAPI CACHE STATISTICS:")
    print("-" * 30)
    print(f"Cache directory: {stats['cache_dir']}")
    print(f"TTL (hours): {stats['ttl_hours']}")
    print(f"Total files: {stats['total_files']}")
    print(f"Valid files: {stats['valid_files']}")
    print(f"Expired files: {stats['expired_files']}")
    
    if stats['expired_files'] > 0:
        print(f"\nRun with --clear-cache to remove expired files")


def cleanup_expired_cache() -> int:
    """
    Clean up expired cache files
    
    Returns:
        Number of expired files removed
    """
    from cache import api_cache
    return api_cache.clear_expired()