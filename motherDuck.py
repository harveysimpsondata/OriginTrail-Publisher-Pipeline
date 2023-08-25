# Standard library
import time
import os
import datetime
from concurrent.futures import ThreadPoolExecutor
import json
from web3 import Web3

# Third-party libraries
import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv

import duckdb

load_dotenv()
subscan_key = os.getenv("subscan_key")
motherDuck_token = os.getenv("motherDuck_token")
onfinality_key = os.getenv("onfinality_key")
subscan_key = os.getenv("subscan_key")
MAX_WORKERS = 3  # adjust this based on your system's capabilities

# Connect to the Ethereum node using Websockets
w3 = Web3(Web3.HTTPProvider(f'https://origintrail.api.onfinality.io/rpc?apikey={onfinality_key}'))

print("Latest Block:", w3.eth.block_number)
latest_block = w3.eth.block_number
last_block = (w3.eth.block_number) - 1

# Load ABI from json file
with open('data/ServiceAgreementV1.json', 'r') as file:
    serviceAgreementABI = json.load(file)

# Contract address and initialization
contract_address = '0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F'
contract = w3.eth.contract(address=contract_address, abi=serviceAgreementABI)


# Fetch past ServiceAgreementV1Created events
events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=last_block, toBlock=last_block)

dicts_list = [dict(event) for event in events_list]

# Flatten the 'args' dictionary and process HexBytes objects
processed_data = []
for item in dicts_list:
    flattened = {**item['args'], **item}  # Merge dictionaries
    flattened.pop('args')  # Remove the 'args' key

    # Convert HexBytes to hexadecimal string representation
    flattened['blockHash'] = flattened['blockHash'].hex()
    flattened['transactionHash'] = flattened['transactionHash'].hex()

    processed_data.append(flattened)

# Create DataFrame
df_assets = (pd.DataFrame(processed_data)
      .assign(tokenAmount=lambda x: x['tokenAmount'].astype(float) / 1e18,
              startTime=lambda x: pd.to_datetime(x['startTime'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())))
      .drop(columns=['keyword', 'hashFunctionId', 'logIndex', 'transactionIndex'])
      .rename(columns={"assetContract":"asset_contract", "startTime": "TIME_ASSET_CREATED", "epochsNumber":"EPOCHS_NUMBER","epochLength":"EPOCH_LENGTH","tokenAmount": "TRAC_PRICE", "event":"EVENT","tokenId": "ASSET_ID", "assetContract":"ASSET_CONTRACT", "transactionHash":"TRANSACTION_HASH", "blockHash":"BLOCK_HASH", "blockNumber":"BLOCK_NUMBER", "address":"EVENT_CONTRACT_ADDRESS"}, errors="raise"))



# Get all transaction hashes
hashes = df_assets['TRANSACTION_HASH'].tolist()

hash_list = []

# Get all transaction hashes
for hash in hashes:
    url = "https://origintrail.api.subscan.io/api/scan/evm/transaction"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": subscan_key
    }
    data = {
        "hash": hash
    }

    response = requests.post(url, headers=headers, json=data).json()

    if response.get("code") == 0:  # only proceed if the response code indicates success
        data = response["data"]
        row = {
            "message": response["message"],
            "generated_at": response["generated_at"],
            "hash": data["hash"],
            "from": data["from"],
            "to": data["to"]["address"]
        }
        hash_list.append(row)

df_hash = ((pd.DataFrame(hash_list)
           .assign(generated_at=lambda x: pd.to_datetime(x['generated_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat()))))
           .rename(columns={"message":"MESSAGE","generated_at":"TIME_OF_TRANSACTION","hash":"TRANSACTION_HASH","from":"PUBLISHER_ADDRESS", "to":"SENT_ADDRESS"}, errors="raise"))

df = pd.merge(df_assets, df_hash, on='TRANSACTION_HASH', how='left')

df = df[['MESSAGE', 'ASSET_ID', 'BLOCK_NUMBER', 'TIME_ASSET_CREATED', 'TIME_OF_TRANSACTION', 'TRAC_PRICE', 'EPOCHS_NUMBER', 'EPOCH_LENGTH', 'PUBLISHER_ADDRESS', 'SENT_ADDRESS', 'TRANSACTION_HASH', 'BLOCK_HASH']]


con = duckdb.connect(database='data/duckDB.db')

con.execute("CREATE TABLE IF NOT EXISTS publishes (MESSAGE VARCHAR(100), ASSET_ID VARCHAR(100), BLOCK_NUMBER INTEGER, TIME_ASSET_CREATED TIMESTAMP, TIME_OF_TRANSACTION TIMESTAMP, TRAC_PRICE FLOAT, EPOCHS_NUMBER INTEGER, EPOCH_LENGTH INTEGER, PUBLISHER_ADDRESS VARCHAR(100), SENT_ADDRESS VARCHAR(100), TRANSACTION_HASH VARCHAR(100) PRIMARY KEY, BLOCK_HASH VARCHAR(100))")

con.execute("""
INSERT INTO publishes
SELECT * FROM df
ON CONFLICT (TRANSACTION_HASH)  -- these are the primary or unique keys
DO NOTHING;
""")


# with duckdb.connect(f'md:origintrail?motherduck_token={motherDuck_token}&saas_mode=true') as conn:
#     try:
#
#         pass
#         # conn.execute(upsert_statement)
#         # print(f"Inserted {len(postgres_data)} rows.")
#     except Exception as e:
#         pass
#         # print(f"Error upserting data: {e}")


