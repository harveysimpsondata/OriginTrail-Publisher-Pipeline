from web3 import Web3
import json
import pandas as pd
import json
import os
onfinality_key = os.getenv("onfinality_key")
# Load ABI from json file
with open('data/ServiceAgreementV1.json', 'r') as file:
    serviceAgreementABI = json.load(file)

#wss://mainnet.infura.io/ws/v3/{key}

# Connect to the Ethereum node using Websockets
w3 = Web3(Web3.HTTPProvider(f'https://origintrail.api.onfinality.io/rpc?apikey={onfinality_key}'))
#w5 = Web3(Web3.WebsocketProvider('wss://origintrail.api.onfinality.io/ws?apikey={key}'))


print("Latest Block:", w3.eth.block_number)
latest = w3.eth.block_number
last = (w3.eth.block_number)-20

print(latest, last)

# Contract address and initialization
contract_address = '0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F'
contract = w3.eth.contract(address=contract_address, abi=serviceAgreementABI)

# Fetch past ServiceAgreementV1Created events
from_block = 0  # for instance, from block 0
to_block = 'latest'
print(contract.events)
events_list = contract.events.ServiceAgreementV1Created.get_logs(fromBlock=last, toBlock=latest)
print(events_list)
dicts_list = [dict(event) for event in events_list]

# Specifically convert the 'args' AttributeDict to a dictionary as well
for d in dicts_list:
    d['args'] = dict(d['args'])

# Convert the list of dictionaries into a DataFrame
df = pd.DataFrame(dicts_list)





