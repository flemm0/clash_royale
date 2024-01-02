from dagster import asset, OpExecutionContext
from dagster_duckdb import DuckDBResource

import requests
import polars as pl
import json
import os

from . import constants

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset(
    deps=['raw_parquet_battle_data']
)
def staging_clan_information_table(context: OpExecutionContext, database: DuckDBResource):
    '''Clash Royale player data sourced from official Clash Royale API'''
    clan_tag_query = f'''
        (
            SELECT DISTINCT "winner.clan.tag" AS clan_tag 
            FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        )
        UNION
        (
            SELECT DISTINCT "loser.clan.tag" AS clan_tag 
            FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        )
    '''
    with database.get_connection() as conn:
        clan_tags = conn.execute(query=clan_tag_query).fetchnumpy()['clan_tag']

    fields = ['tag', 'name', 'type', 'description', 'clanScore', 'clanWarTrophies', 'requiredTrophies', 'members']

    insert_count = 0
    for clan_tag in clan_tags:
        formatted_tag = clan_tag.replace('#', '%23')
        clan_url = f'https://api.clashroyale.com/v1/clans/{formatted_tag}'
        response = requests.get(clan_url, headers)
        if response.status_code == 200:
            data = response.json()
            filtered_data = {k:v for k,v in data.items() if k in fields}
            try:
                filtered_data['location_id'] = data['location']['id']
            except KeyError:
                filtered_data['location_id'] = None
            try:
                filtered_data['location_name'] = data['location']['name']
            except KeyError:
                filtered_data['location_name'] = None
            try:
                filtered_data['is_country'] = data['location']['isCountry']
            except KeyError:
                filtered_data['is_country'] = None

            df = pl.DataFrame(filtered_data)
            insert_query = f'''
                INSERT OR REPLACE INTO stg_clans
                BY NAME (SELECT * FROM df)
            '''
            with database.get_connection() as conn:
                conn.execute(query=insert_query)
            insert_count += 1
            if insert_count % 500 == 0:
                context.log.info(f'Inserted data for {insert_count} clans.')