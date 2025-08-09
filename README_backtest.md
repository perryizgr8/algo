# NSE 200 Momentum Strategy Backtester

This directory contains scripts to backtest the NSE 200 momentum strategies over historical periods.

## Files

### Main Scripts
- **`backtest_strategies.py`** - Full backtester that uses real Upstox API data
- **`test_dryrun.py`** - Dry run test with mock data (no API required)
- **`test_backtest.py`** - Extended mock test (archived)

### Strategy Files
- **`52weeks.ipynb`** - 12-month momentum strategy notebook
- **`26weeks.ipynb`** - 6-month momentum strategy notebook
- **`ind_nifty200list.xlsx`** - NSE 200 stock list with instrument keys

## Quick Start

### 1. Dry Run Test (No API Required)
```bash
python test_dryrun.py
```
This runs a simulation with mock data to verify the backtester logic works correctly.

### 2. Full Backtest (Requires API Token)
```bash
# Set your Upstox API token
set UPSTOX_API_TOKEN=your_token_here

# Run full backtest
python backtest_strategies.py
```

## What the Backtester Does

1. **Historical Analysis**: Simulates running both 6-month and 12-month momentum strategies monthly over the past year
2. **Portfolio Rebalancing**: Each month, it:
   - Calculates returns for all NSE 200 stocks
   - Selects top 20 performers (T20) for buying
   - Keeps holdings that are still in top 40 (T40) 
   - Sells holdings that fall out of T40
3. **Performance Tracking**: Records portfolio value, returns, volatility, and Sharpe ratios
4. **Visualization**: Creates performance comparison charts

## Strategy Logic

### 6-Month Strategy
- Looks at 26-week historical returns
- Rebalances monthly based on recent momentum
- More reactive to short-term trends

### 12-Month Strategy  
- Looks at 52-week historical returns
- Rebalances monthly based on longer-term momentum
- More stable, less sensitive to short-term volatility

## Output

The backtester provides:
- **Total Returns**: Overall performance vs initial capital
- **Annualized Returns**: Risk-adjusted performance metrics
- **Volatility Analysis**: Standard deviation of monthly returns
- **Sharpe Ratios**: Risk-adjusted return ratios
- **Performance Charts**: Visual comparison of both strategies

## Example Output

```
Initial Capital: Rs.1,000,000.00
Period: 2024-02-09 to 2025-02-09
Duration: 1.00 years (12 rebalances)

6-MONTH MOMENTUM STRATEGY:
  Final Value: Rs.1,180,500.00
  Total Return: 18.05%
  Annualized Return: 18.05%

12-MONTH MOMENTUM STRATEGY:
  Final Value: Rs.1,165,200.00
  Total Return: 16.52%
  Annualized Return: 16.52%

RISK METRICS:
  6M Volatility: 12.5%
  12M Volatility: 10.8%
  6M Sharpe Ratio: 1.44
  12M Sharpe Ratio: 1.53
```

## Requirements

- Python 3.7+
- pandas
- numpy
- matplotlib
- requests
- python-dotenv
- openpyxl

## Notes

- The backtester assumes equal-weight allocation across selected stocks
- Transaction costs are not included in the simulation  
- Results are for educational/research purposes only
- Past performance does not guarantee future results