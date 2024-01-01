import polars as pl
import os
import duckdb
import requests

from dagster import asset, OpExecutionContext
from dagster_duckdb import DuckDBResource

from . import constants

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset(
    deps=['raw_parquet_battle_data']
)
def player_data(context: OpExecutionContext, database: DuckDBResource):
    '''Clash Royale player data sourced from official Clash Royale API'''
    player_tag_query = f'''
        (
            SELECT DISTINCT "winner.tag" AS player_tag 
            FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        )
        UNION
        (
            SELECT DISTINCT "loser.tag" AS player_tag 
            FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        )
    '''
    player_tags = duckdb.sql(player_tag_query).fetchnumpy()['player_tag']

    columns = [
        'tag',
        'name',
        'expLevel',
        'trophies',
        'bestTrophies',
        'wins',
        'losses',
        'battleCount',
        'threeCrownWins',
        'role',
        'currentFavoriteCard'
    ]

    for player_tag in player_tags:
        formatted_tag = player_tag.replace('#', '%23')
        player_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}'
        response = requests.get(player_url, headers)
        table = None
        if response.status_code == 200:
            data = response.json()
            # parse fields of interest from API
            filtered_data = {k: v for k, v in data.items() if k in columns}
            try:
                filtered_data['clanTag'] = data['clan']['tag']
            except KeyError:
                filtered_data['clanTag'] = None
            if 'role' not in filtered_data.keys():
                filtered_data['role'] = None
            # read data into Polars DataFrame
            if table is None:
                table = pl.DataFrame(filtered_data)
            else:
                table = table.extend(pl.DataFrame(filtered_data))
            # once DataFrame size reaches 100 MB, write to MotherDuck table
            if table.estimated_size('mb') > 100:
                insert_query = f'''
                    INSERT OR REPLACE INTO stg_players
                    BY NAME (SELECT * FROM table)
                '''
                with database.get_connection() as conn:
                    conn.execute(query=insert_query)
                context.log.info(f'Inserted {table.shape[0]} player records.')
                table = None
