# AGENTS.md - Developer Guide for AI Coding Agents

## Project Overview
This is a Python-based **NSE 200 momentum strategy algorithm** for stock portfolio management. It fetches historical stock data via the Upstox API, calculates returns, and generates buy/sell/hold recommendations for portfolio rebalancing.

### Core Algorithm Strategy
1. **Sort top 20** from NSE 200 by previous 12 months returns (A20)
2. **Invest** in A20
3. **Wait 1 month**
4. **Sort top 20** again by previous 12 months returns (B20)
5. **Invest** in B20
6. **Sell** stocks from A20 that have moved out of top 40

### Algorithm Steps (Detailed)
1. **Load NSE 200 symbols** from Excel file
2. **Calculate returns** for each stock over specified period (6m or 12m)
3. **Sort by returns** in descending order
4. **Select top 40 (T40) and top 20 (T20)**
5. **Compare with current portfolio** to determine:
   - **Buy**: T20 stocks not currently held
   - **Sell**: Current holdings not in T40
   - **Hold**: Current holdings still in T40 but not in T20
6. **Update portfolio CSV** with new holdings

## Build, Test, and Lint Commands

### Running Tests
This project uses standalone test scripts (no formaltest framework like pytest). Run individual tests directly with Python:

```bash
# Run a specific test file
python test_backtest.py          # Test backtest strategy logic
python test_allocation.py        # Test smart allocation strategy  
python test_performance.py      # Test portfolio performance calculations
python test_price_fetching.py   # Test price fetching logic
python test_dryrun.py            # Test dry-run mode
```

**No test framework is configured** - tests are standalone scripts with embedded test logic and print statements.

### Running the Main Algorithm
```bash
# 12-month momentum strategy
python nse200_algorithm.py --strategy 12m

# 6-month momentum strategy
python nse200_algorithm.py --strategy 6m

# Dry run (preview changes without updating files)
python nse200_algorithm.py --strategy 12m --dry-run

# Additional options
python nse200_algorithm.py --strategy 12m --extra-money 50000  # Add ₹50k investment
python nse200_algorithm.py --clear-cache                       # Clear API cache
python nse200_algorithm.py --cache-stats                        # View cache statistics
```

### Linting and Type Checking
**No linting or type checking is currently configured in this project.** 

If you need to add linting/type checking, use:
```bash
# For future use (not currently configured):
pylint *.py              # Lint all Python files
mypy *.py                # Type check (if type hints added)
black *.py               # Format code
```

### Code Formatting
No code formatter is configured. Follow the existing code style (see Code Style Guidelinesbelow).

## Development Environment Setup

### Prerequisites
1. Python 3.8+ (check with `python --version`)
2. Virtual environment (optional but recommended)

### Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Or manually install key dependencies:
pip install pandas requests openpyxl matplotlib tqdm python-dotenv
```

### Environment Variables
Create a `.env` file in theproject root with:
```
UPSTOX_API_TOKEN=your_api_token_here
PORTFOLIO_VALUE=1000000
```

Alternatively, set environment variables in your shell:
```bash
# Windows (PowerShell)
$env:UPSTOX_API_TOKEN = "your_token_here"

# Linux/Mac
export UPSTOX_API_TOKEN="your_token_here"
```

## Code Style Guidelines

### Imports Organization
Organize imports in three sections, separated by blank lines:

```python
# 1. Standard library
import os
import sys
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple

# 2. Third-party libraries
import pandas as pd
import requests
from tqdm import tqdm

# 3. Local modules
from config import API_TOKEN, UPSTOX_BASE_URL
from utils import get_returns, load_nse200_data
from cache import api_cache
```

### Type Hints
Add type hints to function signatures for better code clarity and IDE support:

```python
# ✓ Good - with type hints
def calculate_returns(stock: str, weeks: int) -> Optional[float]:
    """Calculate returns for a stock over specified weeks"""
    pass

def load_current_portfolio(filepath: str) -> pd.DataFrame:
    """Load portfolio from CSV file"""
    pass

# Use Optional[T] for functions that might return None
def get_current_price(instkey: str) -> Optional[float]:
    pass

# Use Tuple for functions returning multiple values
def calculate_units(symbol: str, amount: float) -> Tuple[int, float]:
    pass

# Use List[Dict] for complex data structures
def get_top_stocks(n: int) -> List[Dict[str, any]]:
    pass
```

### Naming Conventions
Follow Python PEP 8 naming conventions:

```python
# Constants
UPSTOX_BASE_URL = "https://api-v2.upstox.com"
TOP_20_COUNT = 20
DEFAULT_PORTFOLIO_VALUE = 1000000

# Variables and functions - snake_case
stock_returns = {}
portfolio_value = 0.0

def calculate_portfolio_value():
    pass

def get_current_price():
    pass

# Classes - PascalCase (CapWords)
class APICache:
    pass

class NSEMomentumBacktester:
    pass

# Private methods - prefix with underscore
def _process_candle_data(response_data: Dict) -> Optional[float]:
    """Private helper method"""
    pass

# Boolean variables - use is_, has_, can_ prefixes
is_valid = True
has_data = False
can_rebalance = True
```

### Docstrings
Use triple-quoted docstrings for all functions and classes:

```python
def calculate_returns_for_all_stocks(weeks: int) -> List[Dict[str, any]]:
    """
    Calculate returns for all NSE 200 stocks
    
    Args:
        weeks: Number of weeks for return calculation
        
    Returns:
        List of dicts with symbol and gain/return data
        
    Raises:
        FileNotFoundError: If NSE200 file is missing
        
    Example:
        >>> returns = calculate_returns_for_all_stocks(52)
        >>> len(returns)
        200
    """
    pass
```

### Error Handling
Handle errors gracefully with informative messages:

```python
# ✓ Good - comprehensive error handling
def get_returns(instkey: str, weeks: int) -> Optional[float]:
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        
        if resp.status_code == 429:  # Rate limit
            time.sleep(2 ** attempt)
            continue
            
        if resp.status_code != 200:
            print(f"API error for {instkey}: {resp.status_code} - {resp.text}")
            return None
            
        rjson = resp.json()
        
        if rjson.get("status") != "success":
            print(f"API status error for {instkey}: {rjson.get('status')}")
            return None
            
        return process_data(rjson)
        
    except requests.exceptions.RequestException as e:
        print(f"Request error for {instkey}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error for {instkey}: {e}")
        return None
```

### Code Comments
- **Minimize comments** - write self-documenting code
- Use docstrings instead of inline comments for function explanations
- Add comments only for non-obvious logic:

```python
# ✓ Good - comment explains WHY, not WHAT
# Exponential backoff to handle rate limiting (429 errors)
time.sleep(2 ** attempt)

# Priority weights: Top stock gets 20x weight, decreases exponentially
weight = 20 * (0.8 ** i)
```

### Line Length
Keep lines under 100 characters. Break long lines with parentheses:

```python
# ✓ Good - line continuation with parentheses
buy_list, sell_list, hold_list = calculate_portfolio_changes(
    current_portfolio, 
    top_40, 
    top_20
)

# ✓ Good - dictionary/list spanning multiple lines
strategies = [
    {"interval": "day", "days_back": 10, "description": "Daily data (10 days)"},
    {"interval": "day", "days_back": 30, "description": "Daily data (30 days)"},
    {"interval": "month", "days_back": 90, "description": "Monthly data (90 days)"},
]
```

### API Token and Secrets
**NEVER commit API tokens or secrets to the repository.** Use environment variables:

```python
# ✓ Good - load from environment
import os
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.getenv('UPSTOX_API_TOKEN')

if not API_TOKEN:
    raise ValueError("UPSTOX_API_TOKEN environment variable is required")

# ✗ Bad - hardcoded token
API_TOKEN = "your_secret_token_here"  # NEVER do this!
```

## Project Structure

```
algo/
├── config.py                # Configuration and API settings
├── utils.py                 # Core utility functions
├── cache.py                 # API response caching system
├── nse200_algorithm.py      # Main algorithm entry point
├── update_nse200.py         # Update NSE 200 list
├── update_nse200_simple.py  # Simplified NSE updater
├── backtest_strategies.py   # Strategy backtesting
├── test_*.py                # Standalone test scripts
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (not in git)
├── portfolio.csv            # 12-month strategy portfolio
├── portfolio6.csv           # 6-month strategy portfolio
├── ind_nifty200list.xlsx    # NSE 200 stock list with Upstox instrument keys
├── 52weeks.ipynb            # 12-month analysis notebook
├── 26weeks.ipynb            # 6-month analysis notebook
├── algo.md                  # Algorithm documentation and strategy description
├── CLAUDE.md                # Project context for AI assistants
└── README.md                # Project overview and quick start
```

**Note**: The project maintains both 6-month (portfolio6.csv) and 12-month (portfolio.csv) momentum strategies separately.

## Testing Guidelines

### Writing New Tests
When adding new test files:

1. **Use descriptive names**: `test_<feature_name>.py`
2. **Add module docstring**: Explain what's being tested
3. **Use clear test function names**: `def test_<specific_behavior>():`
4. **Print results for visibility**: Use `print()` statements for test output
5. **Handle exceptions gracefully**: Use try-except blocks
6. **Test in isolation**: Tests should not require running other scripts first

Example test structure:

```python
#!/usr/bin/env python3
"""
Test script for <feature name>
Tests that <specific behavior being validated>
"""

import sys
import os
from datetime import date, timedelta

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import function_to_test

def test_basic_functionality():
    """Test basic functionality description"""
    print("Test 1: Basic functionality")
    print("-" * 40)
    
    result = function_to_test(param1, param2)
    
    assert result is not None, "Result should not be None"
    assert len(result) > 0, "Result should have data"
    
    print(f"✓ Test passed: {result}")

def test_edge_case():
    """Test edge case handling"""
    print("\nTest 2: Edge case handling")
    print("-" * 40)
    
    result = function_to_test(edge_case_param)
    
    # Handle expected failures gracefully
    if result is None:
        print("✓ Edge case handled correctly (returned None)")
    else:
        print(f"✓ Edge case result: {result}")

if __name__ == "__main__":
    print("Running tests for <feature name>")
    print("=" * 50)
    print()
    
    try:
        test_basic_functionality()
        test_edge_case()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
```

## Common Development Tasks

### Adding New Configuration Parameters
1. Add parameter to `config.py` as a constant (UPPER_CASE)
2. Add environment variable support if needed:

```python
# In config.py
NEW_PARAM = float(os.getenv('NEW_PARAM', '100'))
```

### Adding New Utility Functions
1. Add function to `utils.py`
2. Include type hints and docstring
3. Handle API failures gracefully with retries
4. Use the caching system for API calls:

```python
from cache import api_cache

def new_api_function(instkey: str) -> Optional[Dict]:
    url = f"{UPSTOX_BASE_URL}/endpoint/{instkey}"
    cache_params = {'instkey': instkey}
    
    # Check cache first
    cached = api_cache.get(url, cache_params)
    if cached:
        return cached
        
    # Make API call
    response = requests.get(url, headers=get_api_headers())
    
    # Cache and return
    if response.status_code == 200:
        data = response.json()
        api_cache.set(url, data, cache_params)
        return data
        
    return None
```

### Debugging API Issues
Use the `--debug-prices` flag for detailed output:

```bash
python nse200_algorithm.py --strategy 12m --debug-prices --dry-run
```

### Working with Jupyter Notebooks
The project includes Jupyter notebooks for interactive analysis:
- `52weeks.ipynb` - 12-month strategy analysis
- `26weeks.ipynb` - 6-month strategy analysis

Run notebooks with:
```bash
jupyter notebook 52weeks.ipynb
```

## API Rate Limiting and Caching

The project includes built-in API caching (1-hour TTL) to handle rate limits:

```python
# Cache is automatically used by get_returns()
# Manually control cache:
from cache import api_cache

api_cache.clear_all()        # Clear entire cache
api_cache.clear_expired()     # Clear only expired entries
api_cache.get_cache_stats()   # View cache statistics
```

## Git Commit Guidelines

When creating commits:
- Use clear, descriptive commit messages
- Reference issue numbers if applicable
- Keep commits focused (one logical change per commit)
- Never commit `.env` files or API tokens

Example commit messages:
```
Add smart allocation strategy for expensive stocks
Fix portfolio rebalancing to handle edge cases
Update NSE 200 list with latest constituents
Add type hints to utility functions
```

## Common Issues and Solutions

### "UPSTOX_API_TOKEN not found"
Set the environment variable or create `.env` file with the token.

### "API rate limit exceeded"
- Use `--clear-cache` to remove stale cache data
- The algorithm has built-in exponential backoff (waits 2^n seconds on 429 errors)

### "Module not found"
Run `pip install -r requirements.txt` to install all dependencies.

### "No data for stock X"
Some stocks may have insufficient historical data. The algorithm handles this gracefully by returning None.

## Additional Resources

- **README.md**: Project overview and quick start guide
- **CLAUDE.md**: Project context for AI assistants
- **algo.md**: Detailed algorithm documentation
- **Upstox API Docs**: https://upstox.com/developer/api-documentation/