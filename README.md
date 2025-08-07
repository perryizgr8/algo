# NSE 200 Winner Algorithm

A momentum-based stock selection algorithm for NSE 200 stocks that automatically rebalances portfolios based on historical returns.

## Overview

This algorithm implements a systematic approach to stock selection:

1. **Rank** all NSE 200 stocks by their historical returns (6-month or 12-month)
2. **Select** the top 20 performers for investment
3. **Rebalance** monthly by selling underperformers and buying top performers
4. **Maintain** positions in stocks that stay within the top 40

## Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

### Set Your API Token

Set your Upstox API token as an environment variable:

```bash
# Windows
set UPSTOX_API_TOKEN=your_token_here

# Linux/Mac
export UPSTOX_API_TOKEN=your_token_here
```

Or update the token in `config.py`.

### Run the Algorithm

```bash
# 12-month momentum strategy
python nse200_algorithm.py --strategy 12m

# 6-month momentum strategy  
python nse200_algorithm.py --strategy 6m

# Dry run (preview changes without updating files)
python nse200_algorithm.py --strategy 12m --dry-run

# Clear API cache before running
python nse200_algorithm.py --strategy 12m --clear-cache

# View cache statistics
python nse200_algorithm.py --cache-stats
```

## Files

- **`nse200_algorithm.py`** - Main script to run the algorithm
- **`utils.py`** - Core utility functions for data processing
- **`config.py`** - Configuration settings and parameters
- **`cache.py`** - API response caching system
- **`portfolio.csv`** - 12-month strategy portfolio
- **`portfolio6.csv`** - 6-month strategy portfolio  
- **`ind_nifty200list.xlsx`** - NSE 200 stock list with Upstox keys
- **`52weeks.ipynb`** - Jupyter notebook for 12-month analysis
- **`26weeks.ipynb`** - Jupyter notebook for 6-month analysis
- **`algo.md`** - Detailed algorithm documentation
- **`.cache/`** - Directory for cached API responses (auto-created)

## Algorithm Logic

### Step 1: Data Collection
- Fetch historical price data for all NSE 200 stocks via Upstox API
- Calculate returns over the specified period (6 or 12 months)

### Step 2: Ranking & Selection
- Sort stocks by returns in descending order
- Select top 40 (T40) and top 20 (T20) performers

### Step 3: Portfolio Decisions
- **BUY**: All stocks in T20 (top 20)
- **SELL**: Current holdings NOT in T40 (top 40)
- **HOLD**: Current holdings in T40 but not T20

### Step 4: Portfolio Update
- Update the CSV file with new holdings
- All positions start with 0 units (paper trading)

## Configuration

Edit `config.py` to customize:

- **API_TOKEN**: Your Upstox API authorization token
- **TOP_20_COUNT**: Number of stocks to select for buying (default: 20)
- **TOP_40_COUNT**: Threshold for selling decisions (default: 40)
- **File paths**: Portfolio and data file locations

## API Requirements

This project uses the Upstox API for historical stock data:

- **Endpoint**: `https://api-v2.upstox.com/historical-candle/`
- **Authentication**: Bearer token required
- **Rate Limits**: Built-in retry logic with exponential backoff
- **Documentation**: https://upstox.com/developer/api-documentation/

## Output Example

```
NSE 200 Winner Algorithm - 12M Strategy
Date: 2025-08-07
============================================================

Step 1: Calculating 52-week returns for all NSE 200 stocks...
Processing 1/200: ABB
Processing 2/200: ACC
...

TOP 20 PERFORMERS:
----------------------------------------
 1. STOCK1      +125.45%
 2. STOCK2      +98.32%
 3. STOCK3      +87.21%
...

==================================================
PORTFOLIO REBALANCING SUMMARY
==================================================

BUY (20 stocks):
  1. STOCK1
  2. STOCK2
  ...

SELL (5 stocks):
  1. OLDSTOCK1
  2. OLDSTOCK2
  ...

Portfolio updated: 20 buys, 5 sells
✅ Algorithm completed successfully!
```

## Error Handling

The algorithm includes robust error handling for:

- **API failures**: Automatic retries with exponential backoff
- **Rate limiting**: Built-in delays and retry logic
- **Missing data**: Graceful handling of stocks with no data
- **File errors**: Automatic creation of missing portfolio files

## Caching System

The algorithm includes a sophisticated caching system to improve performance:

- **1-hour TTL**: API responses are cached for 1 hour to avoid redundant calls
- **Automatic cleanup**: Expired cache files are automatically removed
- **Cache commands**:
  - `--cache-stats`: View cache statistics
  - `--clear-cache`: Clear all cached data
- **Storage**: Cache files are stored in `.cache/` directory (auto-created)
- **Benefits**: Faster re-runs, reduced API calls, better rate limit compliance

## Jupyter Notebooks

For interactive analysis, use the included notebooks:

- **`52weeks.ipynb`**: 12-month strategy development and testing
- **`26weeks.ipynb`**: 6-month strategy development and testing

## Disclaimer

This is for educational and research purposes only. Past performance does not guarantee future results. Always do your own research before making investment decisions.