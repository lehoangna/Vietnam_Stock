import vnstock
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta

# Get the current date
now = datetime.now()
current_date_string = now.strftime("%Y-%m-%d")

# Get the date from one day before
one_week_ago = datetime.now() - timedelta(days=7)
one_week_ago_date_string = one_week_ago.strftime('%Y-%m-%d')

# List all ticker
companies = vnstock.listing_companies()
listing_tikers = companies['ticker'].tolist()
error_com = ['CBC', 'C71', 'DAP', 'DTN', 'ROS', 'C36', 'KSS', 'PBK', 'SUM']
dict_data = {
    'Open': [],
    'High': [],
    'Low': [],
    'Close': [],
    'Volume': [],
    'TradingDate': [],
    'Ticker': []
}
intraday_data = pd.DataFrame(dict_data)

# Loop for each symbol
for symbol in listing_tikers[:20]:
    if symbol in error_com:
        continue
    intraday_data_per_symbol = vnstock.stock_historical_data(symbol, one_week_ago_date_string, current_date_string)
    intraday_data_per_symbol['Ticker'] = symbol
    if((intraday_data_per_symbol['TradingDate'].iloc[-1].strftime('%Y-%m-%d')) == current_date_string):
        intraday_data = pd.concat([intraday_data, intraday_data_per_symbol.tail(1)])

# set field 'TradingDate' to right format
intraday_data.set_index('Ticker', inplace=True)
intraday_data['TradingDate'] = intraday_data['TradingDate'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

# Load intraday data to GCS
client = storage.Client.from_service_account_json('vietnamtoken-5dfcd32b407b.json')
bucket = client.get_bucket('2kna_stock_bucket')
bucket.blob(f'upload_vnstock/{current_date_string}.csv').upload_from_string(intraday_data.to_csv(), 'text/csv')