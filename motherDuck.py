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
import pandas as pd
import requests
from web3 import Web3

# service_agreement_v1 contract (where publishing starts)
# 0x4B014C4B8DA1853243fBd9d404F10Db6Aa9FADFc
# 0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F -> ServiceAgreementV1
# 0xFfFFFFff00000000000000000000000000000001 -> TRAC

# Evironment variables
load_dotenv()
SUBSCAN_KEY = os.getenv("SUBSCAN_KEY")
MOTHERDUCK_KEY = os.getenv("MOTHERDUCK_TOKEN")
ONFINALITY_KEY = os.getenv("ONFINALITY_KEY")
MAX_WORKERS = 2  # adjust this based on your system's capabilities


start_time = time.time()
# Connect to the Ethereum node using Websockets
w3 = Web3(Web3.HTTPProvider(f'https://origintrail.api.onfinality.io/rpc?apikey={ONFINALITY_KEY}'))

latest_block = (w3.eth.block_number) - 1
last_blocks = (w3.eth.block_number) - 10

# Load ABI from json file
with open('data/ServiceAgreementV1.json', 'r') as file:
    serviceAgreementABI = json.load(file)

# Contract address and initialization
contract_address = '0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F'
contract = w3.eth.contract(address=contract_address, abi=serviceAgreementABI)

# Fetch past ServiceAgreementV1Created events
events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=3122337, toBlock=3122387)

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


# Create DataFrame
df_assets = (pd.DataFrame(processed_events)
             .assign(tokenAmount=lambda x: x['tokenAmount'].astype(float) / 1e18,
                     epochLength=lambda x: x['epochLength'].astype(float) / 86400,
                     startTime=lambda x: pd.to_datetime(x['startTime']
                                                        .apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())))
             .rename(columns={"assetContract":"ASSET_CONTRACT",
                              "startTime": "TIME_ASSET_CREATED",
                              "epochsNumber":"EPOCHS_NUMBER",
                              "epochLength":"EPOCH_LENGTH-(DAYS)",
                              "tokenAmount": "TRAC_PRICE",
                              "event":"EVENT",
                              "tokenId": "ASSET_ID",
                              "assetContract":"ASSET_CONTRACT",
                              "transactionHash":"TRANSACTION_HASH",
                              "blockHash":"BLOCK_HASH",
                              "blockNumber":"BLOCK_NUMBER",
                              "address":"EVENT_CONTRACT_ADDRESS"},
                     errors="raise"))



# Get all transaction hashes
hashes = df_assets['TRANSACTION_HASH'].tolist()

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

df_hash = ((pd.DataFrame(hash_list)
            .assign(generated_at=lambda x: pd.to_datetime(x['generated_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat()))))
            .rename(columns={"message":"MESSAGE",
                            "generated_at":"TIME_OF_TRANSACTION",
                            "hash":"TRANSACTION_HASH",
                            "from":"PUBLISHER_ADDRESS",
                            "to":"SENT_ADDRESS"},
                    errors="raise"))

df = pd.merge(df_assets, df_hash, on='TRANSACTION_HASH', how='left')

# Filter rows based on the MESSAGE column
df = df[df['MESSAGE'] == 'Success']

df = df[['MESSAGE',
         'ASSET_ID',
         'BLOCK_NUMBER',
         'TIME_ASSET_CREATED',
         'TIME_OF_TRANSACTION',
         'TRAC_PRICE',
         'EPOCHS_NUMBER',
         'EPOCH_LENGTH-(DAYS)',
         'PUBLISHER_ADDRESS',
         'SENT_ADDRESS',
         'TRANSACTION_HASH',
         'BLOCK_HASH']]


con = duckdb.connect(database='data/duckDB.db')

result = con.execute("SELECT MAX(BLOCK_NUMBER) AS max_block FROM publishes").fetchone()

if result:
    max_block_number = result[0]
    print(f"The maximum block number in the database is: {max_block_number}")
else:
    print("Couldn't retrieve the maximum block number.")

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
ON CONFLICT (TRANSACTION_HASH)  -- these are the primary or unique keys
DO NOTHING;
""")

df_length = len(df)

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







