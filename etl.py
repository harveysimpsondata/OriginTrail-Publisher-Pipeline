# Standard library
import time
import os
import datetime
import json

# Third-party libraries
import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv

# SQLAlchemy related
from sqlalchemy import (
    create_engine, Table, Column, Integer, String, MetaData, Float, inspect
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

@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()

# Check database connection using psycopg2
sslmode = 'require'
conn_str = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USERNAME} password={DB_PASSWORD} sslmode={sslmode}"

try:
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"Connected to - {version[0]}")
finally:
    cur.close()
    conn.close()


connection_url = URL.create(
    drivername="postgresql+pg8000",
    username=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

engine = create_engine(connection_url)

meta = MetaData()
publish_table = Table(
    "publishes", meta,
    Column("hash", String, primary_key=True),
    Column("create_at", postgresql.TIMESTAMP, primary_key=True),
    Column("value", Float),
    Column("symbol", String),
    Column("pubber", String)
)


def table_exists(engine, table_name):
    return engine.dialect.has_table(engine.connect(), table_name)


# Create inspector for the engine
inspector = inspect(engine)

# Check if the table "publishes" exists in the database
if "publishes" not in inspector.get_table_names():

    # Create the table if it doesn't exist
    meta.create_all(engine)

    # Create the hypertable
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT create_hypertable('{publish_table.name}', 'create_at', migrate_data => True);")
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

postgres_data = []

for pubber in pubber_list:
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
          .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(
        x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
                  value=lambda x: x['value'].astype(float) / 1e18)
          .drop(columns=['contract', 'decimals', 'name', 'from_display', 'to_display', 'token_id', 'to', 'from'])
          .astype({'hash': str, 'symbol': str, 'pubber': str, 'create_at': 'datetime64[ns]'}))

    postgres_data.extend(df.to_dict(orient='records'))
    print(f"Publisher's Transactions Added! -> Address: {pubber}")

# Upload data to postgres (batch)
# create insert statement
insert_statement = postgresql.insert(publish_table).values(postgres_data)

# if the primary key already exists, update the record
upsert_statement = insert_statement.on_conflict_do_update(
    index_elements=['hash', 'create_at'],
    set_={c.key: c for c in insert_statement.excluded if c.key not in ['hash']})

with session_scope(engine) as conn:
    conn.execute(upsert_statement)
    print(f"Inserted {len(postgres_data)} rows.")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Total execution time: {elapsed_time:.2f} seconds")