# Standard library
import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta

# Third-party libraries
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

# SQLAlchemy related
from sqlalchemy import (
    Column, Float, Integer, MetaData, String, Table, create_engine, func, inspect, select, text
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL



@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(pubber_address_url, transactions_url, API_KEY):

    # Get List of Publishers' Addresses
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }
    data = {
        "contract": "0x5cac41237127f94c2d21dae0b14bfefa99880630",
        "row": 100,
        "page": 0
    }

    response = requests.post(pubber_address_url, headers=headers, json=data).json()
    df = pd.DataFrame(response['data']['list'])
    pubber_list = df['holder'].tolist()

    def fetch_pubber_transactions(pubber):

        # Get List of Transactions for Each Publishers' Address
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": API_KEY
        }
        data = {
            "address": pubber,
            "row": 40
        }

        response = requests.post(transactions_url, headers=headers, json=data).json()

        return pd.DataFrame(response['data']['list'])


    # Use ThreadPoolExecutor to fetch data in parallel
    MAX_WORKERS = 3  # adjust this based on your system's capabilities

    data_list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dfs = list(executor.map(fetch_pubber_transactions, pubber_list))
        data_list = list(zip(dfs, pubber_list))

    return data_list

@task(log_prints=True, retries=3)
def transform_data(data_list):
    transformed_data = []

    for df, pubber in data_list:
        df_transformed = (df.query('symbol == "TRAC"')
                          .query('to == "0x61bb5f3db740a9cb3451049c5166f319a18927eb"')
                          .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(
            x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
                                  value=lambda x: x['value'].astype(float) / 1e18)
                          .drop(
            columns=['contract', 'decimals', 'name', 'from_display', 'to_display', 'token_id', 'to', 'from'])
                          .astype({'hash': str, 'symbol': str, 'pubber': str, 'create_at': 'datetime64[ns]'}))

        transformed_data.extend(df_transformed.to_dict(orient='records'))

    return transformed_data
@task(log_prints=True, retries=3)
def load_data(postgres_data):
    database_block = SqlAlchemyConnector.load("timescale-database")

    with database_block.get_connection(begin=False) as conn:

        # Define the table schema
        meta = MetaData()
        publish_table = Table(
            "publishes", meta,
            Column("hash", String, primary_key=True),
            Column("create_at", postgresql.TIMESTAMP, primary_key=True),
            Column("value", Float),
            Column("symbol", String),
            Column("pubber", String)
        )

        # # Check if the table exists, create it if not, and convert to hypertable
        # inspector = inspect(conn)
        # if "publishes" not in inspector.get_table_names():
        #
        #     # Create the table
        #     meta.create_all(conn)
        #
        #     # Create the hypertable
        #     try:
        #         conn.execute(
        #             text(f"SELECT create_hypertable('{publish_table.name}', 'create_at', migrate_data => True);"))
        #     except Exception as e:
        #         print(f"Error creating hyper table: {e}")


        # Count the rows before the insertion using SQLAlchemy
        initial_count_query = select([func.count()]).select_from(publish_table)
        initial_count = conn.execute(initial_count_query).scalar()

        # Upload data to postgres (batch)
        insert_statement = postgresql.insert(publish_table).values(postgres_data)

        # if the primary key already exists, update the record
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=['hash', 'create_at'],
            set_={c.key: c for c in insert_statement.excluded if c.key not in ['hash']})

        conn.execute(upsert_statement)

        # Count the rows after the insertion using SQLAlchemy
        final_count_query = select([func.count()]).select_from(publish_table)
        final_count = conn.execute(final_count_query).scalar()

        # Calculate the number of inserted rows
        inserted_rows = final_count - initial_count

        return {"status": "success", "inserted_rows": inserted_rows}


@flow(name="Ingest_Flow")
def main_flow():

    load_dotenv()
    API_KEY = os.getenv("API_KEY")

    pubber_address_url = "https://origintrail.api.subscan.io/api/scan/evm/token/holders"
    transactions_url = "https://origintrail.api.subscan.io/api/scan/evm/erc20/transfer"

    raw_data = extract_data(pubber_address_url, transactions_url, API_KEY)
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)

if __name__ == "__main__":
    main_flow()