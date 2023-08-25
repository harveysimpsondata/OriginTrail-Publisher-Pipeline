import requests
from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()
import datetime
API_KEY = os.getenv("API_KEY")


ServiceAgreementV1="0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F"
# ServiceAgreementV1: 0xB20F6F3B9176D4B284bA26b80833ff5bFe6db28F


#Pubber addresses in list

pubber_url = "https://origintrail.api.subscan.io/api/scan/evm/token/holders"
headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY
}
data = {
    "contract": "0x5cac41237127f94c2d21dae0b14bfefa99880630",
    "row": 100,
    "page": 0
}

response = requests.post(pubber_url, headers=headers, json=data).json()

df = pd.DataFrame(response['data']['list'])
pubber_list = df['holder'].tolist()

pubber_test = pubber_list[0:2]


# Step 1: Initialize an empty dataframe
all_df = pd.DataFrame()

for pubber in pubber_test:
    erc20_url = "https://origintrail.api.subscan.io/api/scan/evm/erc20/transfer"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }
    data = {
        "address": pubber,
        "row": 40
    }

    response = requests.post(erc20_url, headers=headers, json=data).json()

    print(response)
    # Extract and process data from the response
    df = (pd.DataFrame(response['data']['list'])
          .query('symbol == "TRAC"')
          .query('to == "0x61bb5f3db740a9cb3451049c5166f319a18927eb"')
          .assign(pubber=pubber, create_at=lambda x: pd.to_datetime(x['create_at'].apply(lambda y: datetime.datetime.utcfromtimestamp(y).isoformat())),
                  value=lambda x: x['value'].astype(float) / 1e18)
          .drop(columns=['contract', 'decimals', 'name', 'from_display', 'to_display', 'token_id', 'to', 'from'])
          .astype({'hash': str, 'symbol': str, 'pubber': str, 'create_at': 'datetime64[ns]'}))

    # Step 2: Append the processed data to the initialized dataframe
    all_df = all_df.append(df, ignore_index=True)


