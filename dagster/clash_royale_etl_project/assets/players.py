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
    deps=['list_of_seasons'],
    metadata={'schema': 'staging', 'table': 'staging.stg_top_players_by_season'}
)
def top_players_by_season(context: OpExecutionContext, database: DuckDBResource) -> None:
    with database.get_connection() as conn:
        seasons = conn.sql('SELECT * FROM staging.stg_seasons').fetchall()
        context.log.info('Successfully queried staging.stg_seasons table')

    insert_into_table_query = '''
        INSERT OR IGNORE INTO staging.stg_top_players_by_season SELECT * FROM df
    '''
    
    for season in seasons:
        response = requests.get(f'{season[1]}?limit=10', headers=headers)
        if response.status_code == 200:
            data = response.json()['items']
            if data:
                df = pl.DataFrame(data)
                df = df.with_columns(pl.lit(season[0]).alias('season_id'))
                with database.get_connection() as conn:
                    conn.execute(query=insert_into_table_query)
                context.log.info(f'Top players for season {season[0]} successfully inserted.')

                
            

    

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