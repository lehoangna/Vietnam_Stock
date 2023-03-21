from stock_data import data
from google.cloud import storage
from google.cloud import bigquery
import os

client = storage.Client.from_service_account_json('vietnamtoken-5dfcd32b407b.json')
bucket = client.get_bucket('2kna_stock_bucket')

bucket.blob('upload_vnstock/data.csv').upload_from_string(data.to_csv(), 'text/csv')
# -----------

client = bigquery.Client.from_service_account_json('vietnamtoken-5dfcd32b407b.json')
# Define your BigQuery dataset and table names
dataset_name = 'stock_dataset'
table_name = 'stock_data'

# Create BigQuery dataset
dataset_ref = client.dataset(dataset_name)
dataset = bigquery.Dataset(dataset_ref)
dataset = client.create_dataset(dataset)

# Define BigQuery schema
schema = [
    bigquery.SchemaField('open', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('high', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('low', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('close', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('volume', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('tradingDate', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('ticker', 'STRING', mode='NULLABLE'),
]

# Create BigQuery table
table_ref = dataset_ref.table(table_name)
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)

data.to_gbq(destination_table=f'{dataset_name}.{table_name}', project_id='vietnamtoken')
