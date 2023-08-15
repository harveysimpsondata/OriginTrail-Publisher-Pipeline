# Standard library
import time
import os
import datetime
from datetime import timedelta
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

from prefect import task, flow
from prefect.tasks import task_input_hash

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(pubber_address_url, transactions_url, API_KEY):


    MAX_WORKERS = 3  # adjust this based on your system's capabilities
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

    def fetch_pubber_transactions(pubber):
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

        df = (pd.DataFrame(response['data']['list']))

        return df

    #     df = (pd.DataFrame(response['data']['list'])
    #           .query('symbol == "TRAC"')
    #           .query('to == "0x61bb5f3db740a9cb3451049c5166f319a18927eb"')
    #           .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(
    #         x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
    #                   value=lambda x: x['value'].astype(float) / 1e18)
    #           .drop(columns=['contract', 'decimals', 'name', 'from_display', 'to_display', 'token_id', 'to', 'from'])
    #           .astype({'hash': str, 'symbol': str, 'pubber': str, 'create_at': 'datetime64[ns]'}))
    #
    #     return df.to_dict(orient='records'), pubber
    #
    # # Use ThreadPoolExecutor to fetch data in parallel
    # postgres_data = []
    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    #     for data, pub in executor.map(fetch_pubber_transactions, pubber_list):
    #         postgres_data.extend(data)
    #         print(f"Publisher's Transactions Added! -> Address: {pub}")
    #
    # return postgres_data

@task(log_prints=True, retries=3)
def transform_data(df, pubber):
    df = (df.query('symbol == "TRAC"')
            .query('to == "0x61bb5f3db740a9cb3451049c5166f319a18927eb"')
            .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
                    value=lambda x: x['value'].astype(float) / 1e18)
            .drop(columns=['contract', 'decimals', 'name', 'from_display', 'to_display', 'token_id', 'to', 'from'])
            .astype({'hash': str, 'symbol': str, 'pubber': str, 'create_at': 'datetime64[ns]'}))



@task(log_prints=True, retries=3)
def load_data(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME):

    @contextmanager
    def session_scope(engine):
        """Provide a transactional scope around a series of operations."""
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    sslmode = 'require'
    conn_str = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USERNAME} password={DB_PASSWORD} sslmode={sslmode}"

    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()

        # Count the rows before the insertion
        cur.execute("SELECT COUNT(*) FROM publishes;")
        initial_count = cur.fetchone()[0]

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

    # Upload data to postgres (batch)
    insert_statement = postgresql.insert(publish_table).values(postgres_data)

    # if the primary key already exists, update the record
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['hash', 'create_at'],
        set_={c.key: c for c in insert_statement.excluded if c.key not in ['hash']})

    with session_scope(engine) as conn:
        conn.execute(upsert_statement)

        # Count the rows after the insertion using SQLAlchemy
        final_count_query = select([func.count()]).select_from(publish_table)
        final_count = conn.execute(final_count_query).scalar()

        # Calculate the number of inserted rows
        inserted_rows = final_count - initial_count

        print(f"Inserted {inserted_rows} rows.")


@flow(name="Ingest_Flow")
def main_flow():
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

    pubber_address_url="https://origintrail.api.subscan.io/api/scan/evm/token/holders"
    transactions_url = "https://origintrail.api.subscan.io/api/scan/evm/erc20/transfer"

    raw_data = extract_data(pubber_address_url, transactions_url, API_KEY)
    transformed_data = transform_data(raw_data)
    load_data(API_KEY, DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

if __name__ == "__main__":
    main_flow()