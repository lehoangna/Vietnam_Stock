import vnstock
import pandas as pd
from datetime import datetime

# get the current date
now = datetime.now()
current_date_string = now.strftime("%Y-%m-%d")

# list all ticker
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

# get data for each symbol
for symbol in listing_tikers:
    if(symbol in error_com): continue
    data_per_symbol = vnstock.stock_historical_data(symbol, '2010-01-01', current_date_string)
    # if 'tradingDate' not in data_per_symbol.columns:
    #     print(f"Error: 'tradingDate' column not found for symbol {symbol}")
    data_per_symbol['Ticker'] = symbol
    data = pd.concat([data, data_per_symbol])

data = data.reset_index(drop=True)
print(data.head())

# df = vnstock.stock_historical_data(listing_tikers[99], '2023-03-10', '2023-03-15')
# print(df.head(5))

# idx = pd.Index(listing_tikers)
# print(idx.get_loc('STW'))
# print(listing_tikers[1238])
