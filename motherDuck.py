# Standard library
import time
import os
import datetime
from concurrent.futures import ThreadPoolExecutor
import json
import Web3

# Third-party libraries
import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv

import duckdb

subscan_key = os.getenv("subscan_key")
motherDuck_token = os.getenv("motherDuck_token")
onfinality_key = os.getenv("onfinality_key")
subscan_key = os.getenv("subscan_key")
MAX_WORKERS = 3  # adjust this based on your system's capabilities

with duckdb.connect(f'md:origintrail?motherduck_token={motherDuck_token}&saas_mode=true') as conn:
    try:
        conn.execute(upsert_statement)
        print(f"Inserted {len(postgres_data)} rows.")
    except Exception as e:
        print(f"Error upserting data: {e}")


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

events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=last_block, toBlock=latest_block)

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
df = pd.DataFrame(processed_data)



