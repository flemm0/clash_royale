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
    '''Top 10 players from each season in Clash Royale starting from 2016-02'''
    with database.get_connection() as conn:
        seasons = conn.sql('SELECT * FROM staging.stg_seasons').fetchall()
        context.log.info('Successfully queried staging.stg_seasons table')

    insert_into_table_query = '''
        INSERT OR IGNORE INTO staging.stg_top_players_by_season SELECT * FROM df;
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
            else:
                context.log.info(f'No data returned for season: {season}')
        else:
            context.log.info(f'Error getting API response: {response.status_code}')

def flatten_json(nested_json):
   flattened_json = {}
   
   def flatten(x, name=''):
      if type(x) is dict:
         for a in x:
            flatten(x[a], name + a + '_')
      else:
         flattened_json[name[:-1]] = x

   flatten(nested_json)
   return flattened_json

def parse_battle_log_data(log: list):
    filtered_log = []
    for battle in log:
        filtered_data = {}
        battle = flatten_json(battle)

        outer_keys = [
            'type', 'battleTime', 'isLadderTournament', 'arena_id', 'arena_name', 
            'gameMode_id', 'gameMode_name', 'deckSelection', 'isHostedMatch', 'leagueNumber'
        ]
        for key in outer_keys:
            try:
                filtered_data[key] = battle[key]
            except KeyError:
                filtered_data[key] = None

        inner_keys = [
            'tag', 'name', 'startingTrophies', 'trophyChange', 'crowns', 'kingTowerHitPoints', 
            'princessTowersHitPoints', 'clan_tag', 'clan_name', 'clan_badgeId', 'cards', 
            'supportCards', 'globalRank', 'elixirLeaked'
        ]
        for key in inner_keys: # for the 'team' and 'opponent' fields, which are highly nested
            try:
                team_key = f'team_{key}'
                filtered_data[team_key] = flatten_json(battle['team'][0])[key]
            except KeyError:
                filtered_data[team_key] = None
            try:
                opponent_key = f'opponent_{key}'
                filtered_data[opponent_key] = flatten_json(battle['opponent'][0])[key]
            except KeyError:
                filtered_data[opponent_key] = None
        
        filtered_data['team_cards'] = [
            {'name': card['name'], 'level': card['level'], 'elixirCost': card['elixirCost']} if card['name'] != 'Mirror' else {'name': card['name'], 'level': card['level'], 'elixirCost': None} for card in filtered_data['team_cards']
        ]
        
        filtered_data['opponent_cards'] = [
            {'name': card['name'], 'level': card['level'], 'elixirCost': card['elixirCost']} if card['name'] != 'Mirror' else {'name': card['name'], 'level': card['level'], 'elixirCost': None} for card in filtered_data['opponent_cards']
        ]
        filtered_log.append(filtered_data)
    return filtered_log

@asset(
        deps=['top_players_by_season'],
        metadata={'schema': 'raw', 'table': 'raw.player_battle_log'}
)
def player_battle_log(context: OpExecutionContext, database: DuckDBResource):
    '''Battle log for each of the top 10 players for every season'''
    with database.get_connection() as conn:
        player_tags = conn.sql('SELECT tag FROM staging.stg_top_players_by_season;').fetchall()
        player_tags = [tag[0] for tag in player_tags]
        context.log.info('Successfully queried player tags from stg_top_players_by_season table.')

    insert_query = '''
        INSERT OR IGNORE INTO raw.player_battle_log BY NAME SELECT * FROM df;
    '''
    
    insertion_count = 0
    for player_tag in player_tags:
        context.log.info(f'{player_tag}')
        formatted_tag = player_tag.replace('#', '%23')
        battle_log_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}/battlelog'
        response = requests.get(battle_log_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                # parse data
                parsed_data = parse_battle_log_data(log=data)
                context.log.info(f'Number of battles: {len(parsed_data)}')
                df = pl.DataFrame(parsed_data)
                with database.get_connection() as conn:
                    conn.execute(query=insert_query)
                insertion_count += 1
                context.log.info(f'Inserted battle log for {player_tag}. Total player insertion count: {insertion_count}')
            else:
                context.log.info(f'No data returned for player: {player_tag}')
        else:
            context.log.info(f'Error getting API response: {response.status_code}')

@asset(
    deps=['player_battle_log'],
    metadata={'schema': 'staging', 'table': 'staging.stg_player_stats'}
)
def player_stats(context: OpExecutionContext, database: DuckDBResource):
    '''Queries top player's overall Clash Royale statistics from RoyaleAPI'''
    # query top player tags
    with database.get_connection() as conn:
        player_tags_query = '''
            (
            SELECT DISTINCT team_tag AS tag
            FROM raw.player_battle_log
            ORDER BY battleTime DESC
            )
            UNION 
            (
            SELECT DISTINCT opponent_tag AS tag
            FROM raw.player_battle_log
            ORDER BY battleTime DESC
            );
        '''
        player_tags = conn.sql(query=player_tags_query).fetchall()
        player_tags = [tag[0] for tag in player_tags]
        context.log.info('Successfully queried player tags from raw player_battle_log table.')

    fields = [
        'tag', 'name', 'expLevel', 'trophies', 'bestTrophies',
        'wins', 'losses', 'battleCount', 'threeCrownWins'
    ]

    insert_query = '''
        INSERT OR REPLACE INTO staging.stg_player_stats BY NAME SELECT * FROM df;
    '''

    player_stats_list, players_missing_data, insertion_count = [], 0, 0
    for player_tag in player_tags:
        formatted_tag = player_tag.replace('#', '%23')
        player_stats_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}'
        response = requests.get(player_stats_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                parsed_data = {k:v for k,v in data.items() if k in fields}
                player_stats_list.append(parsed_data)
                insertion_count += 1
                context.log.info(f'Fetched stats for {player_tag}. Total players fetched: {insertion_count}')
                if len(player_stats_list) == 100:
                    df = pl.DataFrame(player_stats_list)
                    with database.get_connection() as conn:
                        conn.execute(query=insert_query)
                        context.log.info('Sucessfully inserted stats into database')
                    player_stats_list = []
            else:
                context.log.info(f'No data returned for player: {player_tag}')
        elif response.status_code == 404:
            players_missing_data += 1
            context.log.info(f'No data available for player tag: {player_tag}')
        else:
            context.log.info(f'Error getting API response: {response.status_code}')
    if len(player_stats_list):
        df = pl.DataFrame(player_stats_list)
        with database.get_connection() as conn:
            conn.execute(query=insert_query)
            context.log.info('Sucessfully inserted stats into database')
    context.log.info(f'{insertion_count} players successfully inserted.\n{players_missing_data} players have no data available.')