import requests
import os
from dotenv import load_dotenv
import json
import duckdb
import polars as pl
from sys import platform

from dagster import asset, Definitions
from dagster_duckdb import DuckDBResource

load_dotenv()

if platform == 'linux' or platform == 'linux2':
    api_key = os.getenv('PC_CR_API_TOKEN')
elif platform == 'darwin':
    api_key = os.getenv('MAC_CR_API_TOKEN')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset
def locations():
    locations_url = 'https://api.clashroyale.com/v1/locations'
    response = requests.get(url=locations_url, headers=headers).json()
    items = response['items']
    df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE TABLE IF NOT EXISTS stg_locations AS SELECT * FROM df')

@asset
def card_info():
    card_url = 'https://api.clashroyale.com/v1/cards'
    response = requests.get(url=card_url, headers=headers).json()
    items = response['items']
    df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE TABLE IF NOT EXISTS stg_cards AS SELECT * from df')

# configure DuckDB resource
defs = Definitions(
    assets=[locations],
    resources={
        'duckdb': DuckDBResource(database=f'md:?motherduck_token={motherduck_token}')
    }
)


'''
battle log url:
https://api.clashroyale.com/v1/players/{player_tag}/battlelog

rankings of players by season
https://api.clashroyale.com/v1/locations/global/seasons/{season_id e.g. 10}/rankings/players -> player_tag, name, rank
'''