import vnstock
import pandas as pd
from datetime import datetime

# Get the current date
now = datetime.now()
current_date_string = now.strftime("%Y-%m-%d")

# List all ticker
companies = vnstock.listing_companies()
listing_tikers = companies['ticker'].tolist()
error_com = ['CBC', 'C71', 'DAP', 'DTN', 'ROS', 'C36', 'KSS', 'PBK', 'SUM']
list_data = {
    'Open': [],
    'High': [],
    'Low': [],
    'Close': [],
    'Volume': [],
    'TradingDate': [],
    'Ticker': []
}
data = pd.DataFrame(list_data)

# Get data for each symbol from history to now
for symbol in listing_tikers:
    if(symbol in error_com): continue
    data_per_symbol = vnstock.stock_historical_data(symbol, '2010-01-01', current_date_string)
    data_per_symbol['Ticker'] = symbol
    data = pd.concat([data, data_per_symbol])

data = data.reset_index(drop=True)
