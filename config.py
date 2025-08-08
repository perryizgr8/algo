"""
Configuration settings for NSE 200 winner algorithm
"""

import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# API Configuration
UPSTOX_API_VERSION = "2.0"
UPSTOX_BASE_URL = "https://api-v2.upstox.com"

# Get API token from environment variable
API_TOKEN = os.getenv('UPSTOX_API_TOKEN')
if not API_TOKEN:
    raise ValueError("UPSTOX_API_TOKEN environment variable is required")

# File paths
NSE200_FILE = 'ind_nifty200list.xlsx'
PORTFOLIO_12M_FILE = 'portfolio.csv'
PORTFOLIO_6M_FILE = 'portfolio6.csv'

# Algorithm parameters
TOP_20_COUNT = 20
TOP_40_COUNT = 40
WEEKS_12M = 52
WEEKS_6M = 26

# API request headers
def get_api_headers():
    return {
        "Api-Version": UPSTOX_API_VERSION,
        "Accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }

# Date calculation functions
def get_date_range(weeks):
    end_date = str(date.today())
    start_date = str(date.today() - timedelta(weeks=weeks))
    return start_date, end_date