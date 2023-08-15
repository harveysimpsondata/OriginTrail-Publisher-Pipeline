# Standard library
import time
import os
import datetime
from concurrent.futures import ThreadPoolExecutor

# Third-party libraries
import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv

# SQLAlchemy related
from sqlalchemy import (
    create_engine, Table, Column, Integer, String, MetaData, Float, inspect, func, select
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL
from contextlib import contextmanager

start_time = time.time()

os.environ['SQLALCHEMY_SILENCE_UBER_WARNING'] = '1'
os.environ['SQLALCHEMY_WARN_20'] = '1'

load_dotenv()

API_KEY = os.getenv("API_KEY")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
MAX_WORKERS = 3  # adjust this based on your system's capabilities


service_url = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@t9zy9d3gd7.f3gykjdpg2.tsdb.cloud.timescale.com:{DB_PORT}/tsdb_transaction?sslmode=require"

engine = create_engine(service_url, pool_size=10, max_overflow=20, pool_timeout=30)


meta = MetaData()
publish_table = Table(
    "publishes", meta,
    Column("hash", String, primary_key=True),
    Column("create_at", postgresql.TIMESTAMP, primary_key=True),
    Column("value", Float),
    Column("symbol", String),
    Column("pubber", String)
)

# Create inspector for the engine
inspector = inspect(engine)

# Check if the table "publishes" exists in the database
if "publishes" not in inspector.get_table_names():

    # Create the table if it doesn't exist
    meta.create_all(engine)

    # Create the hypertable
    with engine.connect() as conn:
        try:
            conn.execute(f"SELECT create_hypertable('{publish_table.name}', 'create_at', migrate_data => True);")
        except Exception as e:
            print(f"Error creating hyper table: {e}")

url = "https://origintrail.api.subscan.io/api/scan/evm/token/holders"
headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}
data = {
    "contract": "0x5cac41237127f94c2d21dae0b14bfefa99880630",
    "row": 100,
    "page": 0
}

response = requests.post(url, headers=headers, json=data).json()

df = pd.DataFrame(response['data']['list'])
pubber_list = df['holder'].tolist()


def fetch_pubber_data(pubber):
    url = "https://origintrail.api.subscan.io/api/scan/evm/erc20/transfer"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }
    data = {
        "address": pubber,
        "row": 40
    }

    response = requests.post(url, headers=headers, json=data).json()

    df = (pd.DataFrame(response['data']['list'])
          .query('symbol == "TRAC"')
          .query('to == "0x61bb5f3db740a9cb3451049c5166f319a18927eb"')
          .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
                  value=lambda x: x['value'].astype(float) / 1e18)
          .drop(columns=['contract', 'decimals', 'name', 'from_display', 'to_display', 'token_id', 'to', 'from'])
          .astype({'hash': str, 'symbol': str, 'pubber': str, 'create_at': 'datetime64[ns]'}))

    return df.to_dict(orient='records'), pubber


# Use ThreadPoolExecutor to fetch data in parallel
postgres_data = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for data, pub in executor.map(fetch_pubber_data, pubber_list):
        postgres_data.extend(data)
        print(f"Publisher's Transactions Added! -> Address: {pub}")


# Upload data to postgres (batch)
# create insert statement
insert_statement = postgresql.insert(publish_table).values(postgres_data)

# if the primary key already exists, update the record
upsert_statement = insert_statement.on_conflict_do_update(
    index_elements=['hash', 'create_at'],
    set_={c.key: c for c in insert_statement.excluded if c.key not in ['hash']})

with engine.connect() as conn:
    try:
        conn.execute(upsert_statement)
        print(f"Inserted {len(postgres_data)} rows.")
    except Exception as e:
        print(f"Error upserting data: {e}")


end_time = time.time()
elapsed_time = end_time - start_time
print(f"Total execution time: {elapsed_time:.2f} seconds")