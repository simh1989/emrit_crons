import psycopg2
from psycopg2 import sql

from sqlalchemy import create_engine

import pandas as pd

import requests
import json

from discord_webhook import DiscordWebhook


####### Parameters #######
POSTGRES_ADDRESS = 'localhost'
POSTGRES_PORT = 5432
POSTGRES_USERNAME = 'etl'
POSTGRES_PASSWORD = 'etl'
POSTGRES_DBNAME =  'etl'

DISCORD_URL = "https://discord.com/api/webhooks/755075772267626558/jeHPq3kFHj5oAVisPBvZdlez_7p8arBUmMwmwQec3RWbShKJ2Q7PEkR098kihZQ1yc8b"

try:
    # Connect to local database
    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME, 
                                                                                        password=POSTGRES_PASSWORD, 
                                                                                        ipaddress=POSTGRES_ADDRESS, 
                                                                                        port=POSTGRES_PORT, 
                                                                                        dbname=POSTGRES_DBNAME))

    # connecting to a PostgreSQL database
    cnx = create_engine(postgres_str)

    count_blocks = pd.read_sql_query("SELECT count(*) FROM blocks", cnx)
    #count_gateways = pd.read_sql_query("SELECT COUNT(*) FROM gateway_inventory;", cnx)

    etl_height = count_blocks.iloc[0]['count']

    cnx.dispose()
except Exception as e:
    print(e)
    etl_height = "N/A"

helium_explorer_height = requests.get("https://api.helium.io/v1/blocks/height")
if helium_explorer_height.status_code == 200:
    helium_height = helium_explorer_height.json()["data"]["height"]
else:
    helium_height = "N/A"

if helium_height - etl_height >= 10:
    webhook = DiscordWebhook(url=DISCORD_URL, content=f'Helium Blocks: {helium_height} | Emrit ETL Blocks: {etl_height}')
    response = webhook.execute()

