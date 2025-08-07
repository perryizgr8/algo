#!/usr/bin/env python3
"""
NSE 200 Winner Algorithm - Main Script

This script implements the momentum-based stock selection algorithm
for NSE 200 stocks based on historical returns.

Usage:
    python nse200_algorithm.py --strategy 12m  # 12-month strategy
    python nse200_algorithm.py --strategy 6m   # 6-month strategy
"""

import argparse
import sys
from datetime import date
from typing import List, Dict

from config import (
    TOP_20_COUNT, TOP_40_COUNT, WEEKS_12M, WEEKS_6M,
    PORTFOLIO_12M_FILE, PORTFOLIO_6M_FILE
)
from utils import (
    calculate_returns_for_all_stocks,
    load_current_portfolio,
    calculate_portfolio_changes,
    update_portfolio,
    print_portfolio_summary,
    print_top_performers,
    clear_api_cache,
    print_cache_stats,
    cleanup_expired_cache
)


def run_algorithm(strategy: str, dry_run: bool = False) -> None:
    """
    Run the NSE 200 winner algorithm
    
    Args:
        strategy: Either '12m' for 12-month or '6m' for 6-month strategy
        dry_run: If True, show changes without updating files
    """
    print(f"NSE 200 Winner Algorithm - {strategy.upper()} Strategy")
    print(f"Date: {date.today()}")
    print("=" * 60)
    
    # Determine parameters based on strategy
    if strategy == '12m':
        weeks = WEEKS_12M
        portfolio_file = PORTFOLIO_12M_FILE
        print("Running 12-month momentum strategy...")
    elif strategy == '6m':
        weeks = WEEKS_6M
        portfolio_file = PORTFOLIO_6M_FILE
        print("Running 6-month momentum strategy...")
    else:
        print(f"Error: Unknown strategy '{strategy}'. Use '12m' or '6m'")
        sys.exit(1)
    
    try:
        # Step 1: Calculate returns for all NSE 200 stocks
        print(f"\nStep 1: Calculating {weeks}-week returns for all NSE 200 stocks...")
        sorted_stocks = calculate_returns_for_all_stocks(weeks)
        
        if not sorted_stocks:
            print("Error: No stock data retrieved")
            sys.exit(1)
        
        # Step 2: Select top performers
        print(f"\nStep 2: Selecting top performers...")
        top_40 = sorted_stocks[:TOP_40_COUNT]
        top_20 = sorted_stocks[:TOP_20_COUNT]
        
        print_top_performers(sorted_stocks, TOP_20_COUNT)
        
        # Step 3: Load current portfolio
        print(f"\nStep 3: Loading current portfolio from {portfolio_file}...")
        current_portfolio = load_current_portfolio(portfolio_file)
        print(f"Current portfolio has {len(current_portfolio)} stocks")
        
        # Step 4: Calculate portfolio changes
        print(f"\nStep 4: Calculating portfolio changes...")
        buy_list, sell_list, hold_list = calculate_portfolio_changes(
            current_portfolio, top_40, top_20
        )
        
        # Step 5: Display summary
        print_portfolio_summary(buy_list, sell_list, hold_list)
        
        # Step 6: Update portfolio
        if dry_run:
            print(f"\nDRY RUN: Would update {portfolio_file}")
            print("No files were modified.")
        else:
            print(f"\nStep 5: Updating portfolio file...")
            update_portfolio(portfolio_file, buy_list, sell_list)
        
        print(f"\nAlgorithm completed successfully!")
        if not dry_run:
            print(f"Portfolio saved to: {portfolio_file}")
        else:
            print(f"Portfolio file: {portfolio_file} (not modified in dry run)")
        
    except KeyboardInterrupt:
        print("\n\nAlgorithm interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError running algorithm: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="NSE 200 Winner Algorithm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nse200_algorithm.py --strategy 12m    # Run 12-month strategy
  python nse200_algorithm.py --strategy 6m     # Run 6-month strategy
  
The algorithm will:
1. Calculate returns for all NSE 200 stocks
2. Rank them by performance
3. Select top 20 for buying
4. Sell stocks not in top 40
5. Update the portfolio CSV file
        """
    )
    
    parser.add_argument(
        '--strategy', 
        choices=['12m', '6m'],
        help='Strategy to run: 12m (12-month) or 6m (6-month)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without updating portfolio file'
    )
    
    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='Clear all cached API data before running'
    )
    
    parser.add_argument(
        '--cache-stats',
        action='store_true',
        help='Show cache statistics and exit'
    )
    
    args = parser.parse_args()
    
    # Handle cache operations first
    if args.cache_stats:
        print_cache_stats()
        return
    
    if args.clear_cache:
        cleared = clear_api_cache()
        print(f"Cleared {cleared} cache files")
        if not args.strategy:
            return
    
    # Require strategy for actual algorithm execution
    if not args.strategy and not (args.cache_stats or args.clear_cache):
        parser.error("--strategy is required unless using --cache-stats or --clear-cache")
    
    if args.strategy:
        if args.dry_run:
            print("DRY RUN MODE - No files will be modified")
            print()
        
        # Clean up expired cache files before running
        cleanup_expired_cache()
        
        run_algorithm(args.strategy, args.dry_run)


if __name__ == "__main__":
    main()