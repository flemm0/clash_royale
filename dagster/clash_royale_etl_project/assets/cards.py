from dagster import asset
from dagster_duckdb import DuckDBResource

import requests
import os
import polars as pl

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset
def card_info(database: DuckDBResource) -> None:
    '''Extracts and loads Clash Royale card information from API'''
    card_url = 'https://api.clashroyale.com/v1/cards'
    response = requests.get(url=card_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data:
            items = data['items']
            df = pl.DataFrame(items)
    with database.get_connection() as conn:
        conn.execute('CREATE OR REPLACE TABLE stg_cards AS SELECT * from df')