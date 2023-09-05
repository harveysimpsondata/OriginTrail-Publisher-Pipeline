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





con = duckdb.connect(database='data/duckDB.db')


con.execute("""
    CREATE TABLE IF NOT EXISTS tester 
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

result = con.execute("SELECT MAX(BLOCK_NUMBER) AS max_block FROM tester").fetchone()

if result[0] != None:
    max_block_number = result[0]
    print(f"The maximum block number in the database is: {max_block_number}")
else:
    print("Couldn't retrieve the maximum block number.")


print(result[0])









