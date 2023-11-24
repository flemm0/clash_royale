import requests
import os
from dotenv import load_dotenv
import json
import duckdb
import polars as pl

from dagster import asset, Definitions
from dagster_duckdb import DuckDBResource

load_dotenv()

api_key = os.getenv('API_TOKEN')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset
def locations():
    locations_url = 'https://api.clashroyale.com/v1/locations'
    response = requests.get(url=locations_url, headers=headers).json()
    items = response['items']
    df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_locations AS SELECT * FROM df')

@asset
def card_info():
    card_url = 'https://api.clashroyale.com/v1/cards'
    response = requests.get(url=card_url, headers=headers).json()
    items = response['items']
    df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_cards AS SELECT * from df')

@asset
def season_rankings():
    seasons_url = 'https://api.clashroyale.com/v1/locations/global/seasons'
    seasons = requests.get(seasons_url, headers).json()
    data = None
    for season in seasons['items']:
        season_id = season['id']
        rankings_url = f'https://api.clashroyale.com/v1/locations/global/seasons/{season_id}/rankings/players'
        response = requests.get(rankings_url, headers).json()
        if 'items' in response.keys():
            items = response['items']
            df = pl.DataFrame(items)
            df = df.rename({'tag': 'player_tag', 'name': 'player_name'}).unnest('clan')
            df = df.rename({'tag': 'clan_tag', 'name': 'clan_name'})
            df = df.with_columns(pl.lit(season_id).alias('season_id'))
            if data is None:
                data = df
            else:
                data = pl.concat([data, df])
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_season_leaderboards AS SELECT * FROM data')

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