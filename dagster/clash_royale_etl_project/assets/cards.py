from dagster import asset
from dagster_duckdb import DuckDBResource

import requests
import os
import polars as pl

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset(
        metadata={'schema': 'staging', 'table': 'staging.stg_cards'}
)
def card_info(database: DuckDBResource) -> None:
    '''Extracts and loads Clash Royale card information from GitHub API'''
    card_url = 'https://royaleapi.github.io/cr-api-data/json/cards.json'
    response = requests.get(url=card_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data:
            df = pl.DataFrame(data)
            with database.get_connection() as conn:
                conn.execute('CREATE OR REPLACE TABLE staging.stg_cards AS SELECT * from df')