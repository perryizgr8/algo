"""
Utility functions for NSE 200 winner algorithm
"""

import pandas as pd
import requests
from datetime import datetime
import time
from typing import List, Dict, Optional, Tuple
from tqdm import tqdm
from config import get_api_headers, UPSTOX_BASE_URL, NSE200_FILE, PORTFOLIO_VALUE, CASH_RESERVE_PERCENTAGE
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
    
    # Use tqdm for clean progress display
    with tqdm(total=total_stocks, desc="Processing stocks", unit="stock") as pbar:
        for idx, row in df.iterrows():
            symbol = row['Symbol']
            instkey = row['instrument_key']
            
            pbar.set_description(f"Processing {symbol}")
            
            returns = get_returns(instkey, weeks)
            sym_returns[symbol] = returns if returns is not None else 0
            
            pbar.update(1)
            
            # Small delay to avoid rate limiting
            time.sleep(0.05)  # Reduced delay since cache is faster now
    
    print("\nSorting stocks by performance...")
    
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


def smart_allocate_cash(buy_list: List[str], nse200_df: pd.DataFrame, available_cash: float, debug: bool = False) -> Dict[str, float]:
    """
    Smart cash allocation that prioritizes top performers and handles expensive stocks
    
    Args:
        buy_list: List of symbols to buy (in priority order - best performers first)
        nse200_df: NSE200 data with instrument keys
        available_cash: Total cash available for investment
        debug: Enable debug output
        
    Returns:
        Dict mapping symbol to allocation amount
    """
    if debug:
        print(f"Smart allocation: ₹{available_cash:,.2f} across {len(buy_list)} stocks")
    
    # Step 1: Get prices for all stocks
    stock_prices = {}
    min_allocation = available_cash * 0.02  # Minimum 2% allocation per stock
    
    for symbol in buy_list:
        nse_row = nse200_df[nse200_df['Symbol'] == symbol]
        if not nse_row.empty:
            instkey = nse_row.iloc[0]['instrument_key']
            units, price = calculate_units_to_buy(symbol, instkey, min_allocation, use_fallback=True, debug=False)
            if price > 0:
                stock_prices[symbol] = price
    
    if not stock_prices:
        if debug:
            print("No valid prices found, falling back to equal allocation")
        equal_allocation = available_cash / len(buy_list)
        return {symbol: equal_allocation for symbol in buy_list}
    
    # Step 2: Priority-based allocation
    allocations = {}
    remaining_cash = available_cash
    remaining_stocks = list(buy_list)
    
    # Strategy: Ensure every stock gets at least 1 unit if possible
    # Then distribute remaining cash by priority
    
    # Phase 1: Guarantee at least 1 unit for each stock (starting with top performers)
    guaranteed_investment = 0
    affordable_stocks = []
    
    for symbol in buy_list:
        if symbol in stock_prices:
            price = stock_prices[symbol]
            if price <= remaining_cash:
                allocations[symbol] = price  # Allocate exactly 1 unit
                guaranteed_investment += price
                remaining_cash -= price
                affordable_stocks.append(symbol)
                if debug:
                    print(f"  Guaranteed 1 unit of {symbol} @ ₹{price:.2f}")
            else:
                if debug:
                    print(f"  {symbol} too expensive (₹{price:.2f} > ₹{remaining_cash:.2f})")
    
    # Phase 2: Distribute remaining cash proportionally by priority weight
    if remaining_cash > 0 and affordable_stocks:
        # Priority weights: Top stock gets 20x weight, decreases exponentially
        total_weight = 0
        weights = {}
        
        for i, symbol in enumerate(affordable_stocks):
            # Exponential decay: top stock gets highest weight
            weight = 20 * (0.8 ** i)  # 20, 16, 12.8, 10.24, ...
            weights[symbol] = weight
            total_weight += weight
        
        # Distribute remaining cash by weight
        for symbol in affordable_stocks:
            additional_allocation = (weights[symbol] / total_weight) * remaining_cash
            allocations[symbol] += additional_allocation
            
            if debug:
                total_allocation = allocations[symbol]
                units_possible = int(total_allocation // stock_prices[symbol])
                print(f"  {symbol}: ₹{total_allocation:.2f} ({units_possible} units) [weight: {weights[symbol]:.1f}]")
    
    if debug:
        total_allocated = sum(allocations.values())
        print(f"Total allocated: ₹{total_allocated:.2f} ({total_allocated/available_cash*100:.1f}%)")
    
    return allocations


def update_portfolio(portfolio_file: str, buy_list: List[str], sell_list: List[str], extra_money: float = 0, debug_prices: bool = False) -> None:
    """
    Update portfolio CSV file with buy and sell decisions and calculate actual units
    
    Args:
        portfolio_file: Path to portfolio CSV file
        buy_list: List of symbols to buy
        sell_list: List of symbols to sell
        extra_money: Additional money to invest (default: 0)
        debug_prices: Enable debug output for price fetching
    """
    df = load_current_portfolio(portfolio_file)
    nse200_df = load_nse200_data()
    
    # Get existing cash position
    existing_cash = 0
    cash_row = df[df['Symbol'] == 'CASH']
    if not cash_row.empty:
        existing_cash = float(cash_row.iloc[0]['Units'])
    
    # Calculate proceeds from selling positions
    sell_proceeds = 0
    for symbol in sell_list:
        symbol_row = df[df['Symbol'] == symbol]
        if not symbol_row.empty:
            units = float(symbol_row.iloc[0]['Units'])
            if units > 0:
                # Find instrument key
                nse_row = nse200_df[nse200_df['Symbol'] == symbol]
                if not nse_row.empty:
                    instkey = nse_row.iloc[0]['instrument_key']
                    current_price = get_current_price(instkey, debug=debug_prices)
                    if current_price:
                        proceeds = units * current_price
                        sell_proceeds += proceeds
                        print(f"Selling {symbol}: {units} units @ ₹{current_price:.2f} = ₹{proceeds:,.2f}")
    
    # If this is a completely new portfolio (no existing cash or positions)
    if existing_cash == 0 and len(df) == 0:
        base_cash = PORTFOLIO_VALUE * (1 - CASH_RESERVE_PERCENTAGE)
        print(f"New portfolio - using full budget: ₹{PORTFOLIO_VALUE:,.2f}")
    else:
        # Available cash = existing cash + proceeds from sells
        base_cash = existing_cash + sell_proceeds
        print(f"Existing cash: ₹{existing_cash:,.2f}")
        if sell_proceeds > 0:
            print(f"Sell proceeds: ₹{sell_proceeds:,.2f}")
    
    # Add extra money if provided
    if extra_money > 0:
        print(f"Extra money injection: ₹{extra_money:,.2f}")
    
    available_cash = base_cash + extra_money
    print(f"Total available cash for investment: ₹{available_cash:,.2f}")
    
    # Remove sold symbols
    for symbol in sell_list:
        df = df[df['Symbol'] != symbol]
        print(f"Sold all units of {symbol}")
    
    # Smart allocation based on priority and stock prices
    if buy_list:
        print(f"\nCalculating smart allocation for {len(buy_list)} stocks...")
        allocations = smart_allocate_cash(buy_list, nse200_df, available_cash, debug=debug_prices)
        print(f"Smart allocation completed")
    else:
        allocations = {}
    
    # Process buy list - update existing or add new positions
    current_symbols = set(df['Symbol'].values)
    total_investment = 0.0
    
    if not debug_prices and allocations:
        print("\nProcessing buy orders...")
    
    # Use progress bar for buy orders
    buy_iterator = tqdm(buy_list, desc="Processing buys", unit="stock") if not debug_prices else buy_list
    
    for symbol in buy_iterator:
        if symbol not in allocations:
            if debug_prices:
                print(f"No allocation for {symbol}, skipping")
            continue
            
        allocation_amount = allocations[symbol]
        
        # Find instrument key
        nse_row = nse200_df[nse200_df['Symbol'] == symbol]
        if nse_row.empty:
            print(f"Warning: {symbol} not found in NSE200 data, skipping")
            continue
            
        instkey = nse_row.iloc[0]['instrument_key']
        
        if debug_prices:
            print(f"Processing {symbol} with allocation ₹{allocation_amount:.2f}...")
        elif hasattr(buy_iterator, 'set_description'):
            buy_iterator.set_description(f"Processing {symbol}")
        
        # Get units and price in one call to avoid redundant API requests
        units_to_buy, current_price = calculate_units_to_buy(
            symbol, instkey, allocation_amount, use_fallback=True, debug=debug_prices
        )
        
        if units_to_buy > 0 and current_price > 0:
            investment_amount = units_to_buy * current_price
            total_investment += investment_amount
            
            if symbol in current_symbols:
                # Update existing position
                df.loc[df['Symbol'] == symbol, 'Units'] = units_to_buy
                print(f"Updated {symbol}: {units_to_buy} units @ ₹{current_price:.2f} = ₹{investment_amount:,.2f}")
            else:
                # Add new position
                new_row = pd.DataFrame({'Symbol': [symbol], 'Units': [units_to_buy]})
                df = pd.concat([df, new_row], ignore_index=True)
                print(f"Added {symbol}: {units_to_buy} units @ ₹{current_price:.2f} = ₹{investment_amount:,.2f}")
        else:
            if debug_prices:
                print(f"Skipped {symbol}: units={units_to_buy}, price={current_price}, allocation=₹{allocation_amount:.2f}")
            else:
                print(f"Skipped {symbol}: Could not determine units to buy")
    
    # Save updated portfolio
    df.to_csv(portfolio_file, index=False)
    
    remaining_cash = available_cash - total_investment
    print(f"\nInitial portfolio updated: {len(buy_list)} positions, {len(sell_list)} sells")
    print(f"Total invested: ₹{total_investment:,.2f}")
    print(f"Initial remaining cash: ₹{remaining_cash:,.2f}")
    
    # Redistribute remaining cash to minimize leftover cash
    if remaining_cash > 1000:
        print("Redistributing remaining cash...")
    df, final_remaining_cash = redistribute_remaining_cash(df, nse200_df, remaining_cash)
    
    # Update cash position with final remaining amount
    cash_row = df[df['Symbol'] == 'CASH']
    if cash_row.empty:
        cash_row = pd.DataFrame({'Symbol': ['CASH'], 'Units': [final_remaining_cash]})
        df = pd.concat([df, cash_row], ignore_index=True)
    else:
        df.loc[df['Symbol'] == 'CASH', 'Units'] = final_remaining_cash
    
    df.to_csv(portfolio_file, index=False)
    
    print(f"\nFINAL SUMMARY:")
    print(f"Portfolio positions: {len(df[df['Symbol'] != 'CASH'])} stocks")
    print(f"Cash remaining: ₹{final_remaining_cash:,.2f}")
    
    # Calculate cash utilization percentage
    cash_utilization = ((available_cash - final_remaining_cash) / available_cash) * 100
    print(f"Cash utilization: {cash_utilization:.1f}%")


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


def get_current_price(instkey: str, max_retries: int = 3, debug: bool = False) -> Optional[float]:
    """
    Get current price for a stock with multiple fallback strategies
    
    Args:
        instkey: Upstox instrument key
        max_retries: Maximum number of API retries per strategy
        debug: Enable debug output
    
    Returns:
        Current price or None if all strategies failed
    """
    from datetime import date, timedelta
    
    # Strategy 1: Try daily data for last 10 days (handles weekends/holidays)
    strategies = [
        {"interval": "day", "days_back": 10, "description": "Daily data (10 days)"},
        {"interval": "day", "days_back": 30, "description": "Daily data (30 days)"},
        {"interval": "month", "days_back": 90, "description": "Monthly data (90 days)"},
    ]
    
    headers = get_api_headers()
    
    for strategy in strategies:
        if debug:
            print(f"  Trying {strategy['description']} for {instkey}")
        
        end_date = str(date.today())
        start_date = str(date.today() - timedelta(days=strategy['days_back']))
        url = f"{UPSTOX_BASE_URL}/historical-candle/{instkey}/{strategy['interval']}/{end_date}/{start_date}"
        
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                
                if resp.status_code == 429:  # Rate limit
                    if debug:
                        print(f"    Rate limited, waiting {2 ** attempt}s")
                    time.sleep(2 ** attempt)
                    continue
                    
                if resp.status_code != 200:
                    if debug and attempt == max_retries - 1:
                        print(f"    HTTP error: {resp.status_code}")
                    continue
                    
                rjson = resp.json()
                
                if rjson.get("status") != "success":
                    if debug and attempt == max_retries - 1:
                        print(f"    API error: {rjson.get('status')}")
                    continue
                
                candles = rjson.get("data", {}).get("candles", [])
                if candles:
                    # Get the latest closing price (most recent candle)
                    candles.sort(key=datesort, reverse=True)  # Sort by date, newest first
                    latest_price = float(candles[0][4])  # Closing price
                    
                    if debug:
                        print(f"    Success: ₹{latest_price:.2f} using {strategy['description']}")
                    
                    return latest_price
                else:
                    if debug and attempt == max_retries - 1:
                        print(f"    No candle data available")
                
            except requests.exceptions.RequestException as e:
                if debug and attempt == max_retries - 1:
                    print(f"    Request error: {e}")
                time.sleep(2 ** attempt)
            except Exception as e:
                if debug and attempt == max_retries - 1:
                    print(f"    Unexpected error: {e}")
        
        # Small delay between strategies to avoid rate limiting
        time.sleep(0.5)
    
    # All strategies failed
    if debug:
        print(f"  All price strategies failed for {instkey}")
    
    return None


def calculate_portfolio_value(portfolio: pd.DataFrame, nse200_df: pd.DataFrame) -> float:
    """
    Calculate current portfolio value
    
    Args:
        portfolio: Current portfolio DataFrame
        nse200_df: NSE200 data with instrument keys
        
    Returns:
        Total portfolio value
    """
    total_value = 0.0
    
    for _, row in portfolio.iterrows():
        symbol = row['Symbol']
        units = row['Units']
        
        if units == 0:
            continue
            
        # Find instrument key for this symbol
        nse_row = nse200_df[nse200_df['Symbol'] == symbol]
        if nse_row.empty:
            print(f"Warning: {symbol} not found in NSE200 data")
            continue
            
        instkey = nse_row.iloc[0]['instrument_key']
        current_price = get_current_price(instkey)
        
        if current_price is not None:
            total_value += units * current_price
        else:
            print(f"Warning: Could not get price for {symbol}")
    
    return total_value


def calculate_units_to_buy(symbol: str, instkey: str, allocation_amount: float, use_fallback: bool = True, debug: bool = False) -> Tuple[int, float]:
    """
    Calculate how many units to buy with given allocation and return the price used
    
    Args:
        symbol: Stock symbol
        instkey: Upstox instrument key
        allocation_amount: Amount to allocate for this stock
        use_fallback: Use fallback price estimation if API fails
        debug: Enable debug output
        
    Returns:
        Tuple of (units_to_buy, price_used)
    """
    if debug:
        print(f"  Attempting to get price for {symbol} ({instkey})")
    
    current_price = get_current_price(instkey, debug=debug)
    
    if current_price is None and use_fallback:
        if debug:
            print(f"  API failed, trying fallback estimation...")
        # Fallback: Use historical return data to estimate current price
        fallback_price = estimate_price_from_returns(symbol, instkey)
        if fallback_price:
            current_price = fallback_price
            print(f"  Using estimated price for {symbol}: ₹{current_price:.2f} (API failed)")
        elif debug:
            print(f"  Fallback estimation also failed for {symbol}")
    
    if current_price is None:
        if debug:
            print(f"  All price methods failed for {symbol}")
        return 0, 0.0
    
    units = int(allocation_amount // current_price)
    if debug:
        print(f"  {symbol}: ₹{allocation_amount:.2f} ÷ ₹{current_price:.2f} = {units} units")
    
    return units, current_price


def estimate_price_from_returns(symbol: str, instkey: str, weeks: int = 4) -> Optional[float]:
    """
    Estimate current stock price using recent returns data
    
    Args:
        symbol: Stock symbol  
        instkey: Upstox instrument key
        weeks: Number of weeks of data to use for estimation
        
    Returns:
        Estimated current price or None if failed
    """
    try:
        # Get recent returns data (which we know works since it's used in the main algorithm)
        returns_data = get_returns(instkey, weeks)
        
        if returns_data is None:
            return None
        
        # This is a rough estimation based on typical NSE stock price ranges
        # We can improve this later by storing historical prices or using other data
        
        # Estimate based on symbol characteristics (large cap vs mid/small cap)
        large_cap_symbols = {'RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'BHARTIARTL', 'ITC', 'LT'}
        mid_cap_symbols = {'ZOMATO', 'PAYTM', 'POLICYBZR'}
        
        if symbol in large_cap_symbols:
            estimated_price = 2000  # Rough estimate for large cap
        elif symbol in mid_cap_symbols:
            estimated_price = 500   # Rough estimate for mid cap  
        else:
            estimated_price = 1000  # Default estimate
        
        # Adjust based on returns (if stock has done well, might be higher priced)
        if returns_data > 0.5:  # 50%+ returns
            estimated_price *= 1.5
        elif returns_data > 0.2:  # 20%+ returns  
            estimated_price *= 1.2
        elif returns_data < -0.2:  # -20% returns
            estimated_price *= 0.8
        
        return float(estimated_price)
        
    except Exception as e:
        print(f"Price estimation failed for {symbol}: {e}")
        return None


def redistribute_remaining_cash(df: pd.DataFrame, nse200_df: pd.DataFrame, remaining_cash: float, min_cash_threshold: float = 1000) -> Tuple[pd.DataFrame, float]:
    """
    Redistribute remaining cash among existing stock positions to minimize leftover cash
    
    Args:
        df: Current portfolio DataFrame
        nse200_df: NSE200 data with instrument keys
        remaining_cash: Amount of cash to redistribute
        min_cash_threshold: Minimum cash to keep (default: ₹1000)
        
    Returns:
        Tuple of (updated_df, final_remaining_cash)
    """
    if remaining_cash <= min_cash_threshold:
        return df, remaining_cash
    
    cash_to_redistribute = remaining_cash - min_cash_threshold
    print(f"\nRedistributing ₹{cash_to_redistribute:,.2f} among existing positions...")
    
    # Get stock positions (exclude CASH)
    stock_positions = df[df['Symbol'] != 'CASH'].copy()
    if stock_positions.empty:
        return df, remaining_cash
    
    # Create list of (symbol, price, instkey) for stocks that we can buy more of
    buyable_stocks = []
    for _, row in stock_positions.iterrows():
        symbol = row['Symbol']
        
        # Find instrument key
        nse_row = nse200_df[nse200_df['Symbol'] == symbol]
        if not nse_row.empty:
            instkey = nse_row.iloc[0]['instrument_key']
            current_price = get_current_price(instkey, debug=False)
            
            if current_price and current_price <= cash_to_redistribute:
                buyable_stocks.append({
                    'symbol': symbol,
                    'price': current_price,
                    'instkey': instkey,
                    'current_units': row['Units']
                })
    
    if not buyable_stocks:
        print("No stocks affordable with remaining cash")
        return df, remaining_cash
    
    # Sort by price (cheapest first) for better utilization
    buyable_stocks.sort(key=lambda x: x['price'])
    
    redistributed_amount = 0
    redistribution_summary = []
    
    # Keep buying additional shares until we run out of cash
    while cash_to_redistribute >= min(stock['price'] for stock in buyable_stocks):
        # Try to buy one share of each affordable stock in rotation
        bought_this_round = False
        
        for stock in buyable_stocks:
            if cash_to_redistribute >= stock['price']:
                # Buy one more unit of this stock
                cash_to_redistribute -= stock['price']
                redistributed_amount += stock['price']
                
                # Update the DataFrame
                df.loc[df['Symbol'] == stock['symbol'], 'Units'] += 1
                
                # Track for summary
                existing_entry = next((item for item in redistribution_summary if item['symbol'] == stock['symbol']), None)
                if existing_entry:
                    existing_entry['additional_units'] += 1
                    existing_entry['amount'] += stock['price']
                else:
                    redistribution_summary.append({
                        'symbol': stock['symbol'],
                        'additional_units': 1,
                        'amount': stock['price'],
                        'price': stock['price']
                    })
                
                bought_this_round = True
        
        if not bought_this_round:
            break
    
    # Print redistribution summary
    if redistribution_summary:
        print("Cash redistribution summary:")
        for item in redistribution_summary:
            print(f"  {item['symbol']}: +{item['additional_units']} units @ ₹{item['price']:.2f} = ₹{item['amount']:,.2f}")
    
    final_remaining_cash = min_cash_threshold + cash_to_redistribute
    print(f"Redistributed: ₹{redistributed_amount:,.2f}")
    print(f"Final remaining cash: ₹{final_remaining_cash:,.2f}")
    
    return df, final_remaining_cash