# CLAUDE.md - Project Context

## Project Overview
This is an **NSE 200 winner algorithm** project for stock portfolio management. The algorithm implements a momentum-based strategy by selecting top-performing stocks from the NSE 200 index based on historical returns.

## Core Algorithm Strategy
1. **Sort top 20** from NSE 200 by previous 12 months returns (A20)
2. **Invest** in A20
3. **Wait 1 month**
4. **Sort top 20** again by previous 12 months returns (B20)  
5. **Invest** in B20
6. **Sell** stocks from A20 that have moved out of top 40

## Technical Implementation
- **Language**: Python (Jupyter notebooks)
- **Data Source**: Upstox API for historical stock data
- **NSE 200 List**: `ind_nifty200list.xlsx` contains symbols and instrument keys
- **Portfolio Tracking**: `portfolio.csv` and `portfolio6.csv` track current holdings

## Key Files
- **`algo.md`**: Algorithm documentation and strategy description
- **`52weeks.ipynb`**: 12-month returns analysis and portfolio rebalancing
- **`26weeks.ipynb`**: 6-month returns analysis and portfolio rebalancing  
- **`portfolio.csv`**: Current portfolio holdings (12-month strategy)
- **`portfolio6.csv`**: Current portfolio holdings (6-month strategy)
- **`ind_nifty200list.xlsx`**: NSE 200 stock list with Upstox instrument keys
- **`requirements.txt`**: Python dependencies (pandas, requests, openpyxl, jupyter, etc.)

## API Integration
- **Upstox API**: Used for fetching historical candle data
- **Endpoint**: `https://api-v2.upstox.com/historical-candle/{instkey}/month/{end_date}/{start_date}`
- **Authentication**: Bearer token required in headers

## Algorithm Steps (Detailed)
1. **Load NSE 200 symbols** from Excel file
2. **Calculate returns** for each stock over specified period (6m or 12m)
3. **Sort by returns** in descending order
4. **Select top 40 (T40) and top 20 (T20)**
5. **Compare with current portfolio** to determine:
   - **Buy**: T20 stocks not currently held
   - **Sell**: Current holdings not in T40
   - **Hold**: Current holdings still in T40 but not in T20
6. **Update portfolio CSV** with new holdings

## Development Environment
- **Platform**: Windows (`win32`)
- **Working Directory**: `C:\Users\perry\repos\algo`
- **Git Repository**: Yes (master branch)
- **Python Environment**: Jupyter notebook environment with pandas, requests, openpyxl

## Current Status
- Both 26-week and 52-week strategies are implemented
- Portfolio files are maintained separately for each strategy
- Algorithm generates buy/sell/hold recommendations
- All current portfolio holdings show 0 units (paper trading setup)

## Usage Notes
- Run Jupyter notebooks to execute the algorithm
- API token in code may need refresh for live data
- Portfolio CSV files are automatically updated by the algorithm
- Both 6-month and 12-month momentum strategies are available