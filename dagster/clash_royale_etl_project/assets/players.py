import polars as pl
import os
import duckdb
import requests

from dagster import asset, OpExecutionContext
from dagster_duckdb import DuckDBResource

from . import constants

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

# MotherDuck connection for future refactoring
# motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
# conn = duckdb.connect(f'md:?motherduck_token={motherduck_token}')

@asset(
    deps=['raw_parquet_battle_data'],
    metadata={'schema': 'staging', 'table': 'staging.stg_players'}
)
def staging_player_info_table(context: OpExecutionContext, database: DuckDBResource):
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

    fields = [
        'tag', 'name', 'expLevel', 'trophies', 'bestTrophies',
        'wins', 'losses', 'battleCount', 'threeCrownWins', 'role', 'currentFavoriteCard'
    ]

    insert_count = 0
    for player_tag in player_tags:
        formatted_tag = player_tag.replace('#', '%23')
        player_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}'
        response = requests.get(player_url, headers)

        # table = None

        insert_query = f'''
                BEGIN TRANSACTION;
                INSERT OR REPLACE INTO staging.stg_players SELECT * FROM df;
                COMMIT;
            '''

        if response.status_code == 200:
            data = response.json()
            if data:
                # parse fields of interest from API
                filtered_data = {k: v for k, v in data.items() if k in fields}
                try:
                    filtered_data['clanTag'] = data['clan']['tag']
                except KeyError:
                    filtered_data['clanTag'] = None
                try:
                    filtered_data['role'] = data['role']
                except KeyError:
                    filtered_data['role'] = None

                # read data into Polars DataFrame
                df = pl.DataFrame(filtered_data)

                ## single line insertion
                with database.get_connection() as conn:
                    conn.execute(query=insert_query)
                insert_count += 1
                if insert_count % 5 == 0:
                    context.log.info(f'Inserted data for {insert_count} players.')
                
                ## bulk insertion
                # if table is None:
                #     table = df
                # else:
                #     table = table.extend(df)
                # if table.estimated_size('mb') > 500:
                #     with database.get_connection() as conn:
                #         conn.execute(query=insert_query)
                #     table = None
                #     insert_count += 500
                #     if insert_count % 1000 == 0:
                #         context.log.info(f'Inserted data for {insert_count} players.')