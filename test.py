from web3 import Web3
import json

w3 = Web3(Web3.WebsocketProvider('wss://parachain-rpc.origin-trail.network'))

w3_ = Web3(Web3.HTTPProvider('https://astrosat.origintrail.network'))

print(w3_.is_connected())




"""service_agreement_v1 contract (where publishing starts) 
0x4B014C4B8DA1853243fBd9d404F10Db6Aa9FADFc 
0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F"""
trac="0xFfFFFFff00000000000000000000000000000001"

#contract = w3.eth.contract(address='0xFfFFFFff00000000000000000000000000000001')

#contract = w3.eth.contract(address='0x4B014C4B8DA1853243fBd9d404F10Db6Aa9FADFc')

#contract = w3.eth.contract(address='0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F', abi='/data/ServiceAgreementV1.json')

#print(contract.functions.getServiceAgreementList().call())



# Load the ABI from the ServiceAgreementV1.json file
with open("data/ServiceAgreementV1.json", "r") as file:
    contract_data = json.load(file)
    contract_abi = contract_data["abi"]

# Connect to Ethereum
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID'))

# Ensure you're connected to the Ethereum node
if not w3.isConnected():
    raise ValueError("Not connected to Ethereum node")

# Instantiate the contract using the provided address and ABI
contract_address = "0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F"
contract_instance = w3.eth.contract(address=contract_address, abi=contract_abi)

# Now you can interact with the contract using the contract_instance object
