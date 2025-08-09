#!/usr/bin/env python3
"""
NSE 200 Momentum Strategy Backtester
Simulates 6-month and 12-month momentum strategies over the past year with monthly rebalancing
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import requests
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

class NSEMomentumBacktester:
    def __init__(self):
        self.api_token = os.getenv('UPSTOX_API_TOKEN')
        if not self.api_token:
            raise ValueError("UPSTOX_API_TOKEN environment variable is not set")
        
        self.headers = {
            "Api-Version": "2.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }
        
        # Load NSE 200 list
        self.nse200_df = pd.read_excel('ind_nifty200list.xlsx')
        print(f"Loaded {len(self.nse200_df)} NSE 200 stocks")
        
        # Initialize portfolios
        self.portfolio_6m = {}  # symbol: units
        self.portfolio_12m = {}  # symbol: units
        
        # Track performance
        self.performance_6m = []
        self.performance_12m = []
        self.rebalance_dates = []
        
        # Initial capital
        self.initial_capital = 1000000  # 10 lakh
        self.capital_6m = self.initial_capital
        self.capital_12m = self.initial_capital

    def datesort(self, crow):
        dt = crow[0]
        return int(datetime.fromisoformat(dt).timestamp())

    def get_historical_returns(self, instkey, weeks_back, end_date):
        """Get returns for a stock over specified weeks looking back from end_date"""
        ed = end_date.strftime('%Y-%m-%d')
        sd = (end_date - timedelta(weeks=weeks_back)).strftime('%Y-%m-%d')
        url = f"https://api-v2.upstox.com/historical-candle/{instkey}/month/{ed}/{sd}"
        
        try:
            resp = requests.get(url, headers=self.headers)
            if resp.status_code != 200:
                print(f"API error for {instkey}: {resp.status_code}")
                return 0
                
            rjson = resp.json()
            if rjson["status"] != "success":
                print(f"API status error for {instkey}: {rjson['status']}")
                return 0
            
            candles = rjson["data"]["candles"]
            if not candles:
                return 0
                
            candles.sort(key=self.datesort, reverse=True)
            
            start_price = candles[-1][4]
            end_price = candles[0][4]
            return (end_price - start_price) / start_price
            
        except Exception as e:
            print(f"Error fetching data for {instkey}: {e}")
            return 0

    def get_current_price(self, instkey, date_):
        """Get current price for a stock at a specific date"""
        ed = date_.strftime('%Y-%m-%d')
        sd = (date_ - timedelta(days=7)).strftime('%Y-%m-%d')
        url = f"https://api-v2.upstox.com/historical-candle/{instkey}/day/{ed}/{sd}"
        
        try:
            resp = requests.get(url, headers=self.headers)
            if resp.status_code != 200:
                return 100  # Default price
                
            rjson = resp.json()
            if rjson["status"] != "success":
                return 100
            
            candles = rjson["data"]["candles"]
            if not candles:
                return 100
                
            return candles[0][4]  # Latest close price
            
        except Exception as e:
            return 100

    def get_top_stocks(self, weeks_back, end_date, top_n=20):
        """Get top N stocks based on returns over specified weeks"""
        print(f"Calculating {weeks_back}-week returns as of {end_date.strftime('%Y-%m-%d')}")
        
        sym_returns = {}
        for _, row in self.nse200_df.iterrows():
            symbol = row['Symbol']
            instkey = row['instrument_key']
            returns = self.get_historical_returns(instkey, weeks_back, end_date)
            sym_returns[symbol] = {
                'returns': returns,
                'instkey': instkey
            }
        
        # Sort by returns
        sorted_stocks = sorted(sym_returns.items(), key=lambda x: x[1]['returns'], reverse=True)
        
        top_stocks = sorted_stocks[:top_n]
        top_40 = sorted_stocks[:40]
        
        print(f"Top {top_n} stocks by {weeks_back}-week returns:")
        for i, (symbol, data) in enumerate(top_stocks[:10]):
            print(f"  {i+1}. {symbol}: {data['returns']:.2%}")
        
        return top_stocks, top_40

    def rebalance_portfolio(self, strategy, top_20, top_40, current_date):
        """Rebalance portfolio based on strategy"""
        if strategy == '6m':
            portfolio = self.portfolio_6m
            capital_attr = 'capital_6m'
        else:
            portfolio = self.portfolio_12m
            capital_attr = 'capital_12m'
        
        current_capital = getattr(self, capital_attr)
        
        # Get current holdings
        current_holdings = set(portfolio.keys())
        top_20_symbols = {stock[0] for stock in top_20}
        top_40_symbols = {stock[0] for stock in top_40}
        
        # Determine actions
        sell_stocks = current_holdings - top_40_symbols
        buy_stocks = top_20_symbols - current_holdings
        hold_stocks = current_holdings & top_40_symbols & top_20_symbols
        
        print(f"\n{strategy.upper()} Strategy Rebalancing on {current_date.strftime('%Y-%m-%d')}:")
        print(f"  Sell: {len(sell_stocks)} stocks")
        print(f"  Buy: {len(buy_stocks)} stocks") 
        print(f"  Hold: {len(hold_stocks)} stocks")
        
        # Calculate portfolio value before rebalancing
        portfolio_value = 0
        for symbol in list(portfolio.keys()):
            if symbol in sell_stocks:
                # Get instkey for selling stock
                instkey = self.nse200_df[self.nse200_df['Symbol'] == symbol]['instrument_key'].iloc[0]
                price = self.get_current_price(instkey, current_date)
                units = portfolio[symbol]
                stock_value = units * price
                portfolio_value += stock_value
                current_capital += stock_value
                del portfolio[symbol]
            else:
                instkey = self.nse200_df[self.nse200_df['Symbol'] == symbol]['instrument_key'].iloc[0]
                price = self.get_current_price(instkey, current_date)
                units = portfolio[symbol]
                portfolio_value += units * price
        
        # Add remaining cash
        portfolio_value += current_capital
        
        # Buy new stocks - equal weight allocation
        if buy_stocks:
            capital_per_stock = current_capital / len(top_20_symbols)
            
            for symbol in buy_stocks:
                instkey = self.nse200_df[self.nse200_df['Symbol'] == symbol]['instrument_key'].iloc[0]
                price = self.get_current_price(instkey, current_date)
                units = capital_per_stock / price
                portfolio[symbol] = units
                current_capital -= capital_per_stock
        
        setattr(self, capital_attr, current_capital)
        
        print(f"  Portfolio value: Rs.{portfolio_value:,.2f}")
        print(f"  Cash remaining: Rs.{current_capital:,.2f}")
        
        return portfolio_value

    def run_backtest(self):
        """Run the backtest simulation"""
        print("Starting NSE 200 Momentum Strategy Backtest")
        print("=" * 50)
        
        # Generate monthly rebalance dates for the past year
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        
        current_date = start_date
        month_count = 0
        
        while current_date <= end_date:
            month_count += 1
            self.rebalance_dates.append(current_date)
            
            print(f"\nMonth {month_count}: {current_date.strftime('%Y-%m-%d')}")
            print("-" * 30)
            
            # Get top stocks for both strategies
            top_20_6m, top_40_6m = self.get_top_stocks(26, current_date, 20)
            top_20_12m, top_40_12m = self.get_top_stocks(52, current_date, 20)
            
            # Rebalance portfolios
            portfolio_value_6m = self.rebalance_portfolio('6m', top_20_6m, top_40_6m, current_date)
            portfolio_value_12m = self.rebalance_portfolio('12m', top_20_12m, top_40_12m, current_date)
            
            # Record performance
            self.performance_6m.append(portfolio_value_6m)
            self.performance_12m.append(portfolio_value_12m)
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        self.analyze_results()

    def analyze_results(self):
        """Analyze and display backtest results"""
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS SUMMARY")
        print("=" * 60)
        
        # Calculate returns
        initial_value = self.initial_capital
        final_value_6m = self.performance_6m[-1]
        final_value_12m = self.performance_12m[-1]
        
        total_return_6m = (final_value_6m - initial_value) / initial_value
        total_return_12m = (final_value_12m - initial_value) / initial_value
        
        # Annualized returns
        days_elapsed = (self.rebalance_dates[-1] - self.rebalance_dates[0]).days
        years_elapsed = days_elapsed / 365.25
        
        annualized_return_6m = (final_value_6m / initial_value) ** (1 / years_elapsed) - 1
        annualized_return_12m = (final_value_12m / initial_value) ** (1 / years_elapsed) - 1
        
        print(f"Initial Capital: Rs.{initial_value:,.2f}")
        print(f"Period: {self.rebalance_dates[0]} to {self.rebalance_dates[-1]}")
        print(f"Duration: {years_elapsed:.2f} years ({len(self.rebalance_dates)} rebalances)")
        print()
        
        print("6-MONTH MOMENTUM STRATEGY:")
        print(f"  Final Value: Rs.{final_value_6m:,.2f}")
        print(f"  Total Return: {total_return_6m:.2%}")
        print(f"  Annualized Return: {annualized_return_6m:.2%}")
        print()
        
        print("12-MONTH MOMENTUM STRATEGY:")
        print(f"  Final Value: Rs.{final_value_12m:,.2f}")
        print(f"  Total Return: {total_return_12m:.2%}")
        print(f"  Annualized Return: {annualized_return_12m:.2%}")
        print()
        
        # Monthly returns
        monthly_returns_6m = []
        monthly_returns_12m = []
        
        for i in range(1, len(self.performance_6m)):
            monthly_ret_6m = (self.performance_6m[i] - self.performance_6m[i-1]) / self.performance_6m[i-1]
            monthly_ret_12m = (self.performance_12m[i] - self.performance_12m[i-1]) / self.performance_12m[i-1]
            monthly_returns_6m.append(monthly_ret_6m)
            monthly_returns_12m.append(monthly_ret_12m)
        
        if monthly_returns_6m:
            volatility_6m = np.std(monthly_returns_6m) * np.sqrt(12)
            volatility_12m = np.std(monthly_returns_12m) * np.sqrt(12)
            
            sharpe_6m = annualized_return_6m / volatility_6m if volatility_6m > 0 else 0
            sharpe_12m = annualized_return_12m / volatility_12m if volatility_12m > 0 else 0
            
            print("RISK METRICS:")
            print(f"  6M Volatility: {volatility_6m:.2%}")
            print(f"  12M Volatility: {volatility_12m:.2%}")
            print(f"  6M Sharpe Ratio: {sharpe_6m:.2f}")
            print(f"  12M Sharpe Ratio: {sharpe_12m:.2f}")
        
        # Create performance chart
        self.plot_performance()

    def plot_performance(self):
        """Plot performance comparison"""
        plt.figure(figsize=(12, 8))
        
        # Convert to returns
        returns_6m = [(val / self.initial_capital - 1) * 100 for val in self.performance_6m]
        returns_12m = [(val / self.initial_capital - 1) * 100 for val in self.performance_12m]
        
        plt.plot(self.rebalance_dates, returns_6m, label='6-Month Strategy', linewidth=2)
        plt.plot(self.rebalance_dates, returns_12m, label='12-Month Strategy', linewidth=2)
        plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        
        plt.title('NSE 200 Momentum Strategies - Performance Comparison', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Returns (%)', fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig('strategy_performance.png', dpi=300, bbox_inches='tight')
        print(f"\nPerformance chart saved as 'strategy_performance.png'")
        plt.show()

def main():
    """Main function to run the backtest"""
    try:
        backtester = NSEMomentumBacktester()
        backtester.run_backtest()
    except Exception as e:
        print(f"Error running backtest: {e}")
        print("Make sure you have:")
        print("1. Set UPSTOX_API_TOKEN environment variable")
        print("2. ind_nifty200list.xlsx file in the current directory")
        print("3. Required Python packages installed (pandas, numpy, matplotlib, requests)")

if __name__ == "__main__":
    main()