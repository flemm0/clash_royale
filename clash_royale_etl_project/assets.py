import requests
import os
from dotenv import load_dotenv
import json
import duckdb
import polars as pl

from dagster import asset, Definitions
from dagster_duckdb import DuckDBResource

load_dotenv()

api_key = os.getenv('PC_API_KEY')
motherduck_token = os.getenv('MOTHERDUCK_KEY')
headers = {'Authorization': f'Bearer {api_key}'}

@asset
def locations():
    locations_url = 'https://api.clashroyale.com/v1/locations'
    response = requests.get(url=locations_url, headers=headers).json()
    items = response['items']
    df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE TABLE IF NOT EXISTS stg_locations AS SELECT * FROM df')


# configure DuckDB resource
defs = Definitions(
    assets=[locations],
    resources={
        'duckdb': DuckDBResource(database=f'md:?motherduck_token={motherduck_token}')
    }
)