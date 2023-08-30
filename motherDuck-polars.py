# Standard library imports
import datetime
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# Third-party imports
from dotenv import load_dotenv
import duckdb
import polars as pl
import requests
from web3 import Web3

# service_agreement_v1 contract (where publishing starts)
# 0x4B014C4B8DA1853243fBd9d404F10Db6Aa9FADFc
# 0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F -> ServiceAgreementV1
# 0xFfFFFFff00000000000000000000000000000001 -> TRAC

# Environment variables
load_dotenv()
SUBSCAN_KEY = os.getenv("subscan_key")
MOTHERDUCK_KEY = os.getenv("motherDuck_token")
ONFINALITY_KEY = os.getenv("onfinality_key")
MAX_WORKERS = 2  # adjust this based on your system's capabilities

start_time = time.time()

# Connect to the Ethereum node using Websockets
w3 = Web3(Web3.HTTPProvider(f'https://origintrail.api.onfinality.io/rpc?apikey={ONFINALITY_KEY}'))

# Connect to database
con = duckdb.connect(database='data/duckdb.db')

database_block = con.execute("""
    SELECT MAX(BLOCK_NUMBER) 
    AS max_block 
    FROM publishes
""").fetchone()

if database_block:
    max_block_number = (database_block[0]) - 1
    print(f"The maximum block number in the database is: {max_block_number}")
else:
    max_block_number = 0
    print("Couldn't retrieve the maximum block number.")

print(f"The latest block number is: {w3.eth.block_number}")
print(f"The last 500 blocks are: {w3.eth.block_number - 500}")
latest_block = w3.eth.block_number - 1
last_block_500 = w3.eth.block_number - 500

# Load ABI from json file
with open('data/ServiceAgreementV1.json', 'r') as file:
    serviceAgreementABI = json.load(file)

# Contract address and initialization
contract_address = '0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F'
contract = w3.eth.contract(address=contract_address, abi=serviceAgreementABI)

if (max_block_number > last_block_500) and database_block:
    # Fetch past ServiceAgreementV1Created events
    events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=max_block_number, toBlock=latest_block)
else:
    # Fetch past ServiceAgreementV1Created events
    events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=last_block_500, toBlock=latest_block)

# Fetch past ServiceAgreementV1Created events
# events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=3121337, toBlock=3122337)

if len(events_list) > 0:
    processed_events = [{
        'assetContract': item['args'].get('assetContract', ''),
        'startTime': item['args'].get('startTime', ''),
        'epochsNumber': item['args'].get('epochsNumber', ''),
        'epochLength': item['args'].get('epochLength', ''),
        'tokenAmount': item['args'].get('tokenAmount', ''),
        'event': item.get('event', ''),
        'tokenId': item['args'].get('tokenId', ''),
        'transactionHash': item.get('transactionHash', '').hex() if item.get('transactionHash') else '',
        'blockHash': item.get('blockHash', '').hex() if item.get('blockHash') else '',
        'blockNumber': item.get('blockNumber', ''),
        'address': item.get('address', '')
    } for item in events_list]
else:
    print("No events found for the specified blocks.")
    sys.exit()  # Exit the program

# Create DataFrame using polars
df_assets = (
    pl.DataFrame(processed_events)
    .with_columns([
        (pl.col("tokenAmount") / 1e18).alias("tokenAmount"),
        (pl.col("epochLength") / 86400).alias("epochLength"),
        pl.col("startTime").apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat()).alias("startTime")
    ])
    .select([
        pl.col("assetContract").alias("ASSET_CONTRACT"),
        pl.col("startTime").alias("TIME_ASSET_CREATED"),
        pl.col("epochsNumber").alias("EPOCHS_NUMBER"),
        pl.col("epochLength").alias("EPOCH_LENGTH-(DAYS)"),
        pl.col("tokenAmount").alias("TRAC_PRICE"),
        pl.col("event").alias("EVENT"),
        pl.col("tokenId").alias("ASSET_ID"),
        pl.col("transactionHash").alias("TRANSACTION_HASH"),
        pl.col("blockHash").alias("BLOCK_HASH"),
        pl.col("blockNumber").alias("BLOCK_NUMBER"),
        pl.col("address").alias("EVENT_CONTRACT_ADDRESS")
    ]))

# Get all transaction hashes
hashes = df_assets['TRANSACTION_HASH'].to_list()


def fetch_transaction_data(hash):
    subscan_url = "https://origintrail.api.subscan.io/api/scan/evm/transaction"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": SUBSCAN_KEY
    }
    data = {
        "hash": hash
    }
    response = requests.post(subscan_url, headers=headers, json=data).json()
    if response.get("code") == 0:
        data = response["data"]
        return {
            "message": response["message"],
            "generated_at": response["generated_at"],
            "hash": data["hash"],
            "from": data["from"],
            "to": data["to"]["address"]
        }


with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    hash_list = list(executor.map(fetch_transaction_data, hashes))

# Filter out any None values from the hash_list
hash_list = [h for h in hash_list if h is not None]

df_hash = (
    pl.DataFrame(hash_list)
    .with_columns(
        pl.col("generated_at")
        .apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat()).alias("generated_at")
    )
    .select([
        pl.col("message").alias("MESSAGE"),
        pl.col("generated_at").alias("TIME_OF_TRANSACTION"),
        pl.col("hash").alias("TRANSACTION_HASH"),
        pl.col("from").alias("PUBLISHER_ADDRESS"),
        pl.col("to").alias("SENT_ADDRESS")
            ]))

df = df_assets.join(df_hash, on="TRANSACTION_HASH", how="left")

# Filter rows based on the MESSAGE column
df = df.filter(pl.col("MESSAGE") == "Success")

df = df.select(["MESSAGE",
                "ASSET_ID",
                "BLOCK_NUMBER",
                "TIME_ASSET_CREATED",
                "TIME_OF_TRANSACTION",
                "TRAC_PRICE",
                "EPOCHS_NUMBER",
                "EPOCH_LENGTH-(DAYS)",
                "PUBLISHER_ADDRESS",
                "SENT_ADDRESS",
                "TRANSACTION_HASH",
                "BLOCK_HASH"])

con.execute("""
    CREATE TABLE IF NOT EXISTS publishes 
    (MESSAGE VARCHAR(100), 
    ASSET_ID VARCHAR(100), 
    BLOCK_NUMBER INTEGER, 
    TIME_ASSET_CREATED TIMESTAMP, 
    TIME_OF_TRANSACTION TIMESTAMP, 
    TRAC_PRICE FLOAT, 
    EPOCHS_NUMBER INTEGER, 
    EPOCH_LENGTH_DAYS FLOAT, 
    PUBLISHER_ADDRESS VARCHAR(100), 
    SENT_ADDRESS VARCHAR(100), 
    TRANSACTION_HASH VARCHAR(100) PRIMARY KEY, 
    BLOCK_HASH VARCHAR(100))
""")

con.execute("""
    INSERT INTO publishes
    SELECT * FROM df
    ON CONFLICT (TRANSACTION_HASH)  -- this is the primary key
    DO NOTHING;
""")

df_length = df.height
print(f"Inserted {df_length} rows.")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Total execution time: {elapsed_time:.2f} seconds")

# with duckdb.connect(f'md:origintrail?motherduck_token={motherDuck_token}&saas_mode=true') as conn:
#     try:
#
#         pass
#         # conn.execute(upsert_statement)
#         # print(f"Inserted {len(postgres_data)} rows.")
#     except Exception as e:
#         pass
#         # print(f"Error upserting data: {e}")
