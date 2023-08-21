# Standard library
import os
import datetime
from concurrent.futures import ThreadPoolExecutor

# Third-party libraries
import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import flow, task
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Float
)
from sqlalchemy.dialects import postgresql

@task(log_prints=True, retries=3)
def extract_publishing_addresses(API_KEY):

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

    return pubber_list

@task(log_prints=True, retries=3)
def fetch_address_transactions(API_KEY, MAX_WORKERS, pubber_list):
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
              .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(
            x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
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

    return postgres_data


@task(log_prints=True, retries=3)
def load_to_postgres(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, postgres_data):

    service_url = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    engine = create_engine(service_url)

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


@flow(name="OriginTrail Pipeline")
def main_flow():

    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_PGBOUNCER_PORT = os.getenv("DB_PGBOUNCER_PORT")
    DB_PGBOUNCER_HOST = os.getenv("DB_PGBOUNCER_HOST")
    DB_NAME = os.getenv("DB_NAME")
    MAX_WORKERS = 2

    publisher_list = extract_publishing_addresses(API_KEY)
    transactions = fetch_address_transactions(API_KEY, MAX_WORKERS, publisher_list)
    load_to_postgres(DB_USERNAME, DB_PASSWORD, DB_PGBOUNCER_HOST, DB_PGBOUNCER_PORT, DB_NAME, transactions)

if __name__ == "__main__":

    main_flow()