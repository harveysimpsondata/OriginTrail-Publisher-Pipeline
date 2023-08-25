# Standard library
import time
import os
import datetime
from concurrent.futures import ThreadPoolExecutor

# Third-party libraries
import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv

import duckdb

subscan_key = os.getenv("subscan_key")
motherDuck_token = os.getenv("motherDuck_token")
onfinality_key = os.getenv("onfinality_key")
MAX_WORKERS = 3  # adjust this based on your system's capabilities


