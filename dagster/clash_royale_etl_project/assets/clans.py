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
        clan_tags_query = '''
            SELECT DISTINCT clan.tag
            FROM staging.stg_top_players_by_season
            WHERE clan.tag IS NOT NULL;
        '''
        clan_tags = conn.sql(query=clan_tags_query).fetchall()
        clan_tags = [tag[0] for tag in clan_tags]
        context.log.info('Successfully queried clan tags from stg_top_players_by_season table.')

    fields = ['tag', 'name', 'type', 'description', 'clanScore', 'clanWarTrophies', 'requiredTrophies', 'members']

    insert_query = '''
        INSERT OR REPLACE INTO staging.stg_clan_stats BY NAME SELECT * FROM df;
    '''

    clan_stats_list, clans_missing_data, insertion_count = [], 0, 0
    for clan_tag in clan_tags:
        formatted_tag = clan_tag.replace('#', '%23')
        clan_url = f'https://api.clashroyale.com/v1/clans/{formatted_tag}'
        response = requests.get(clan_url, headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                # parse fields of interest from API
                parsed_data = {k:v for k,v in data.items() if k in fields}
                try:
                    parsed_data['location_id'] = data['location']['id']
                except KeyError:
                    parsed_data['location_id'] = None
                try:
                    parsed_data['location_name'] = data['location']['name']
                except KeyError:
                    parsed_data['location_name'] = None
                try:
                    parsed_data['is_country'] = data['location']['isCountry']
                except KeyError:
                    parsed_data['is_country'] = None
                clan_stats_list.append(parsed_data)
                insertion_count += 1
                context.log.info(f'Fetched clan stats for {clan_tag}. Total clans fetched: {insertion_count}')
                # write out data once 100 items in list
                if len(clan_stats_list) == 100:
                    df = pl.DataFrame(clan_stats_list)
                    with database.get_connection() as conn:
                        conn.execute(query=insert_query)
                        context.log.info(f'Successfully inserted stats into database.')
                    clan_stats_list = []
            else:
                context.log.info(f'No data returned for clan: {clan_tag}')
        elif response.status_code == 404:
            clans_missing_data += 1
            context.log.info(f'No data available for clan tag: {clan_tag}')
        else:
            context.log.info(f'Error getting API response: {response.status_code}')
    # write out remaining data
    if len(clan_stats_list):
        df = pl.DataFrame(clan_stats_list)
        with database.get_connection() as conn:
            conn.execute(query=insert_query)
            context.log.info('Sucessfully inserted stats into database')
    context.log.info(f'{insertion_count} clans successfully inserted.\n{clans_missing_data} clans have no data available.')