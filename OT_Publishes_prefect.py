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
from prefect import flow, task

@task(log_prints=True, retries=3, retry_delay_seconds=5, tags=['exatract-events'])
def extract_events(con, serviceAgreementABI, ONFINALITY_KEY):
    # Connect to the Ethereum node using Websockets
    w3 = Web3(Web3.HTTPProvider(f'https://origintrail.api.onfinality.io/rpc?apikey={ONFINALITY_KEY}'))

    database_block = con.execute("""
        SELECT MAX(BLOCK_NUMBER) 
        AS max_block 
        FROM publishes
    """).fetchone()

    if database_block[0] is not None:
        max_block_number = (database_block[0]) - 1
        print(f"The maximum block number in the database is: {max_block_number + 1}")
    else:
        max_block_number = 0
        print("Couldn't retrieve the maximum block number.")

    print(f"The latest block number is: {w3.eth.block_number}")

    latest_block = w3.eth.block_number - 1
    last_block_500 = w3.eth.block_number - 500

    # Contract address and initialization
    contract_address = '0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F'
    contract = w3.eth.contract(address=contract_address, abi=serviceAgreementABI)

    if (max_block_number > last_block_500) and database_block[0] is not None:
        # Fetch past ServiceAgreementV1Created events
        events_list = contract.events.ServiceAgreementV1Created.get_logs(
            fromBlock=max_block_number,
            toBlock=latest_block
        )

    else:
        # Fetch past 500 ServiceAgreementV1Created events
        events_list = contract.events.ServiceAgreementV1Created.get_logs(
            fromBlock=last_block_500,
            toBlock=latest_block
        )

    # TEST!!
    # Use to test the script Comment out when running in production
    events_list = contract.events.ServiceAgreementV1Created.get_logs(
        fromBlock=last_block_500 + 497,
        toBlock=latest_block
    )

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
        print("No events found for the specified block(s).")
        sys.exit()  # Exit the program

    return processed_events

@task(log_prints=True, retries=3, retry_delay_seconds=5, tags=['create-dataframe'])
def create_dataframe(processed_events, SUBSCAN_KEY, MAX_WORKERS):
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

    return df


@task(log_prints=True, retries=3, tags=['load-to-motherduck'])
def load_to_motherduck(df, con):

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

@flow(name="OriginTrail Pipeline")
def ot_flow():

    load_dotenv()
    SUBSCAN_KEY = os.getenv("SUBSCAN_KEY")
    ONFINALITY_KEY = os.getenv("ONFINALITY_KEY")
    MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
    MAX_WORKERS = 2  # adjust this based on your system's capabilities

    serviceAgreementABI = [
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "hubAddress",
        "type": "address"
      }
    ],
    "stateMutability": "nonpayable",
    "type": "constructor"
  },
  {
    "inputs": [],
    "name": "ScoreError",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint8",
        "name": "scoreFunctionId",
        "type": "uint8"
      }
    ],
    "name": "ScoreFunctionDoesntExist",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "TooLowAllowance",
    "type": "error"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "TooLowBalance",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "ZeroEpochsNumber",
    "type": "error"
  },
  {
    "inputs": [],
    "name": "ZeroTokenAmount",
    "type": "error"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "internalType": "address",
        "name": "assetContract",
        "type": "address"
      },
      {
        "indexed": True,
        "internalType": "uint256",
        "name": "tokenId",
        "type": "uint256"
      },
      {
        "indexed": False,
        "internalType": "bytes",
        "name": "keyword",
        "type": "bytes"
      },
      {
        "indexed": False,
        "internalType": "uint8",
        "name": "hashFunctionId",
        "type": "uint8"
      },
      {
        "indexed": False,
        "internalType": "uint256",
        "name": "startTime",
        "type": "uint256"
      },
      {
        "indexed": False,
        "internalType": "uint16",
        "name": "epochsNumber",
        "type": "uint16"
      },
      {
        "indexed": False,
        "internalType": "uint128",
        "name": "epochLength",
        "type": "uint128"
      },
      {
        "indexed": False,
        "internalType": "uint96",
        "name": "tokenAmount",
        "type": "uint96"
      }
    ],
    "name": "ServiceAgreementV1Created",
    "type": "event"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "indexed": False,
        "internalType": "uint16",
        "name": "epochsNumber",
        "type": "uint16"
      }
    ],
    "name": "ServiceAgreementV1Extended",
    "type": "event"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "indexed": False,
        "internalType": "uint96",
        "name": "tokenAmount",
        "type": "uint96"
      }
    ],
    "name": "ServiceAgreementV1RewardRaised",
    "type": "event"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      }
    ],
    "name": "ServiceAgreementV1Terminated",
    "type": "event"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "indexed": False,
        "internalType": "uint96",
        "name": "updateTokenAmount",
        "type": "uint96"
      }
    ],
    "name": "ServiceAgreementV1UpdateRewardRaised",
    "type": "event"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "assetOwner",
        "type": "address"
      },
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "internalType": "uint96",
        "name": "tokenAmount",
        "type": "uint96"
      }
    ],
    "name": "addTokens",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "assetOwner",
        "type": "address"
      },
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "internalType": "uint96",
        "name": "tokenAmount",
        "type": "uint96"
      }
    ],
    "name": "addUpdateTokens",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "commitManagerV1",
    "outputs": [
      {
        "internalType": "contract CommitManagerV1",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "commitManagerV1U1",
    "outputs": [
      {
        "internalType": "contract CommitManagerV1U1",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "components": [
          {
            "internalType": "address",
            "name": "assetCreator",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "assetContract",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "tokenId",
            "type": "uint256"
          },
          {
            "internalType": "bytes",
            "name": "keyword",
            "type": "bytes"
          },
          {
            "internalType": "uint8",
            "name": "hashFunctionId",
            "type": "uint8"
          },
          {
            "internalType": "uint16",
            "name": "epochsNumber",
            "type": "uint16"
          },
          {
            "internalType": "uint96",
            "name": "tokenAmount",
            "type": "uint96"
          },
          {
            "internalType": "uint8",
            "name": "scoreFunctionId",
            "type": "uint8"
          }
        ],
        "internalType": "struct ServiceAgreementStructsV1.ServiceAgreementInputArgs",
        "name": "args",
        "type": "tuple"
      }
    ],
    "name": "createServiceAgreement",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "assetOwner",
        "type": "address"
      },
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "internalType": "uint16",
        "name": "epochsNumber",
        "type": "uint16"
      },
      {
        "internalType": "uint96",
        "name": "tokenAmount",
        "type": "uint96"
      }
    ],
    "name": "extendStoringPeriod",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "sender",
        "type": "address"
      },
      {
        "internalType": "address",
        "name": "assetContract",
        "type": "address"
      },
      {
        "internalType": "uint256",
        "name": "tokenId",
        "type": "uint256"
      },
      {
        "internalType": "uint16",
        "name": "epoch",
        "type": "uint16"
      }
    ],
    "name": "getChallenge",
    "outputs": [
      {
        "internalType": "bytes32",
        "name": "assertionId",
        "type": "bytes32"
      },
      {
        "internalType": "uint256",
        "name": "challenge",
        "type": "uint256"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "internalType": "uint16",
        "name": "epoch",
        "type": "uint16"
      }
    ],
    "name": "getTopCommitSubmissions",
    "outputs": [
      {
        "components": [
          {
            "internalType": "uint72",
            "name": "identityId",
            "type": "uint72"
          },
          {
            "internalType": "uint72",
            "name": "prevIdentityId",
            "type": "uint72"
          },
          {
            "internalType": "uint72",
            "name": "nextIdentityId",
            "type": "uint72"
          },
          {
            "internalType": "uint40",
            "name": "score",
            "type": "uint40"
          }
        ],
        "internalType": "struct ServiceAgreementStructsV1.CommitSubmission[]",
        "name": "",
        "type": "tuple[]"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "hashingProxy",
    "outputs": [
      {
        "internalType": "contract HashingProxy",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "hub",
    "outputs": [
      {
        "internalType": "contract Hub",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "initialize",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "internalType": "uint16",
        "name": "epoch",
        "type": "uint16"
      }
    ],
    "name": "isCommitWindowOpen",
    "outputs": [
      {
        "internalType": "bool",
        "name": "",
        "type": "bool"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      },
      {
        "internalType": "uint16",
        "name": "epoch",
        "type": "uint16"
      }
    ],
    "name": "isProofWindowOpen",
    "outputs": [
      {
        "internalType": "bool",
        "name": "",
        "type": "bool"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "name",
    "outputs": [
      {
        "internalType": "string",
        "name": "",
        "type": "string"
      }
    ],
    "stateMutability": "pure",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "parametersStorage",
    "outputs": [
      {
        "internalType": "contract ParametersStorage",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "proofManagerV1",
    "outputs": [
      {
        "internalType": "contract ProofManagerV1",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "proofManagerV1U1",
    "outputs": [
      {
        "internalType": "contract ProofManagerV1U1",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "scoringProxy",
    "outputs": [
      {
        "internalType": "contract ScoringProxy",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "components": [
          {
            "internalType": "address",
            "name": "assetContract",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "tokenId",
            "type": "uint256"
          },
          {
            "internalType": "bytes",
            "name": "keyword",
            "type": "bytes"
          },
          {
            "internalType": "uint8",
            "name": "hashFunctionId",
            "type": "uint8"
          },
          {
            "internalType": "uint16",
            "name": "epoch",
            "type": "uint16"
          },
          {
            "internalType": "bytes32[]",
            "name": "proof",
            "type": "bytes32[]"
          },
          {
            "internalType": "bytes32",
            "name": "chunkHash",
            "type": "bytes32"
          }
        ],
        "internalType": "struct ServiceAgreementStructsV1.ProofInputArgs",
        "name": "args",
        "type": "tuple"
      }
    ],
    "name": "sendProof",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "serviceAgreementStorageProxy",
    "outputs": [
      {
        "internalType": "contract ServiceAgreementStorageProxy",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "bool",
        "name": "_status",
        "type": "bool"
      }
    ],
    "name": "setStatus",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "status",
    "outputs": [
      {
        "internalType": "bool",
        "name": "",
        "type": "bool"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "components": [
          {
            "internalType": "address",
            "name": "assetContract",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "tokenId",
            "type": "uint256"
          },
          {
            "internalType": "bytes",
            "name": "keyword",
            "type": "bytes"
          },
          {
            "internalType": "uint8",
            "name": "hashFunctionId",
            "type": "uint8"
          },
          {
            "internalType": "uint16",
            "name": "epoch",
            "type": "uint16"
          }
        ],
        "internalType": "struct ServiceAgreementStructsV1.CommitInputArgs",
        "name": "args",
        "type": "tuple"
      }
    ],
    "name": "submitCommit",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "assetOwner",
        "type": "address"
      },
      {
        "internalType": "bytes32",
        "name": "agreementId",
        "type": "bytes32"
      }
    ],
    "name": "terminateAgreement",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "tokenContract",
    "outputs": [
      {
        "internalType": "contract IERC20",
        "name": "",
        "type": "address"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "version",
    "outputs": [
      {
        "internalType": "string",
        "name": "",
        "type": "string"
      }
    ],
    "stateMutability": "pure",
    "type": "function"
  }
]


    with duckdb.connect(f'md:origintrail?motherduck_token={MOTHERDUCK_TOKEN}&saas_mode=true') as con:
    #with duckdb.connect(database='data/duckDB.db') as con:
        event_list = extract_events(con, serviceAgreementABI, ONFINALITY_KEY)
        df = create_dataframe(event_list, SUBSCAN_KEY, MAX_WORKERS)
        motherduck = load_to_motherduck(df, con)


if __name__ == "__main__":
    ot_flow()


