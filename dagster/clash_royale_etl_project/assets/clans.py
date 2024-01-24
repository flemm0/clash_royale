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
    metadata={'schema': 'staging', 'table': 'stg_clan_stats'},
    deps=['top_players_by_season']
)
def clan_stats(context: OpExecutionContext, database: DuckDBResource):
    '''Clash Royale clan data sourced from official Clash Royale API'''
    with database.get_connection() as conn:
        clan_tags = conn.sql('SELECT clan.tag FROM staging.stg_top_players_by_season WHERE clan.tag IS NOT NULL;').fetchall()
        clan_tags = [tag[0] for tag in clan_tags]
        context.log.info('Successfully queried clan tags from stg_top_players_by_season table.')

    fields = ['tag', 'name', 'type', 'description', 'clanScore', 'clanWarTrophies', 'requiredTrophies', 'members']

    insert_query = '''
        BEGIN TRANSACTION;
        INSERT OR IGNORE INTO staging.stg_clan_stats BY NAME SELECT * FROM df;
        COMMIT;
    '''

    insertion_count = 0
    for clan_tag in clan_tags:
        formatted_tag = clan_tag.replace('#', '%23')
        clan_url = f'https://api.clashroyale.com/v1/clans/{formatted_tag}'
        response = requests.get(clan_url, headers)

        if response.status_code == 200:
            data = response.json()
            if data:
                # parse fields of interest from API
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
                
                # read data into Polars DataFrame
                df = pl.DataFrame(filtered_data)
                with database.get_connection() as conn:
                    conn.execute(query=insert_query)
                insertion_count += 1
                context.log.info(f'Inserted stats for {clan_tag}. Total player insertion count: {insertion_count}')
            else:
                context.log.info(f'No data returned for clan: {clan_tag}')
        else:
            context.log.info(f'Error getting API response: {response.status_code}')