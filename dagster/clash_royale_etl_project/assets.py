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
def locations() -> None:
    '''Extracts and loads Clash Royale location data from API'''
    locations_url = 'https://api.clashroyale.com/v1/locations'
    response = requests.get(url=locations_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data:
            items = data['items']
            df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_locations AS SELECT * FROM df')

@asset
def card_info() -> None:
    '''Extracts and loads Clash Royale card information from API'''
    card_url = 'https://api.clashroyale.com/v1/cards'
    response = requests.get(url=card_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data:
            items = data['items']
            df = pl.DataFrame(items)
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_cards AS SELECT * from df')

@asset
def season_rankings() -> list:
    '''Extracts and loads top players from each Clash Royale season'''
    seasons_url = 'https://api.clashroyale.com/v1/locations/global/seasons'
    seasons_data = requests.get(seasons_url, headers).json()
    tbl = None
    for season in seasons_data['items']:
        season_id = season['id']
        rankings_url = f'https://api.clashroyale.com/v1/locations/global/seasons/{season_id}/rankings/players'
        response = requests.get(rankings_url, headers)
        data = response.json()
        if 'items' in data.keys():
            items = data['items']
            df = pl.DataFrame(items)
            df = df.rename({'tag': 'player_tag', 'name': 'player_name'}).unnest('clan')
            df = df.rename({'tag': 'clan_tag', 'name': 'clan_name'})
            df = df.with_columns(pl.lit(season_id).alias('season_id'))
            if tbl is None:
                tbl = df
            else:
                tbl = pl.concat([tbl, df])

    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_season_leaderboards AS SELECT * FROM tbl')

    return tbl['player_tag'].to_list()

@asset
def player_data(season_rankings: list):
    '''Takes in player tags from `season_rankings()` and extracts player data from API'''
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS stg_player_data(
            tag VARCHAR(50) PRIMARY KEY,
            name VARCHAR(50),
            expLevel INTEGER,
            trophies INTEGER,
            bestTrophies INTEGER,
            wins INTEGER,
            losses INTEGER,
            battleCount INTEGER,
            threeCrownWins INTEGER,
            challengeCardsWon INTEGER,
            challengeMaxWins INTEGER,
            tournamentCardsWon INTEGER,
            tournamentBattleCount INTEGER,
            role VARCHAR(50),
            donations INTEGER,
            donationsReceived INTEGER,
            totalDonations INTEGER,
            warDayWins INTEGER,
            clanTag VARCHAR(50),
            arenaName VARCHAR(50),
            currentSeasonTrophies INTEGER,
            currentSeasonTrophiesBest INTEGER,
            bestSeasonId VARCHAR(50),
            bestSeasonTrophies VARCHAR(50),
            currentFavoriteCard VARCHAR(20)
        );
    '''
    con.execute(query=create_table_query)

    cursor = con.cursor()
    cursor.execute('SELECT tag FROM stg_player_data')
    existing_players = {row[0] for row in cursor.fetchall()}

    cols = [
        'tag',
        'name',
        'expLevel',
        'trophies',
        'bestTrophies',
        'wins',
        'losses',
        'battleCount',
        'threeCrownWins',
        'challengeCardsWon',
        'challengeMaxWins',
        'tournamentCardsWon',
        'tournamentBattleCount',
        'role',
        'donations',
        'donationsReceived',
        'totalDonations',
        'warDayWins'
    ]
    for player_tag in set(season_rankings).difference(existing_players):
        formatted_tag = player_tag.replace('#', '%23')
        player_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}'
        response = requests.get(player_url, headers)
        if response.status_code == 200:
            data = response.json()
            filtered_data = {k: v for k, v in data.items() if k in cols}
            try:
                filtered_data['clanTag'] = data['clan']['tag']
            except KeyError:
                filtered_data['clanTag'] = None
            filtered_data['arenaName'] = data['arena']['name']
            try:
                filtered_data['currentSeasonTrophies'] = data['leagueStatistics']['currentSeason']['trophies']
            except KeyError:
                filtered_data['currentSeasonTrophies'] = None
            try:
                filtered_data['currentSeasonTrophiesBest'] = data['leagueStatistics']['currentSeason']['bestTrophies']
            except KeyError:
                filtered_data['currentSeasonTrophiesBest'] = None
            try:
                filtered_data['bestSeasonId'] = data['leagueStatistics']['bestSeason']['id']
            except KeyError:
                filtered_data['bestSeasonId'] = None
            try:
                filtered_data['bestSeasonTrophies'] = data['leagueStatistics']['bestSeason']['trophies']
            except KeyError:
                filtered_data['bestSeasonTrophies'] = None
            try:
                filtered_data['currentFavoriteCard'] = data['currentFavouriteCard']['name']
            except KeyError:
                filtered_data['currentFavoriteCard'] = None
            # generate placeholders for query parameters 
            placeholders = ','.join('?' * len(filtered_data))
            insert_query = f'''
                INSERT INTO stg_player_data ({','.join(filtered_data.keys())})
                VALUES ({placeholders})
            '''
            con.execute(insert_query, list(filtered_data.values()))

def unnest_all(df: pl.DataFrame, seperator="_"):
    def _unnest_all(struct_columns):
        return df.with_columns(
            [
                pl.col(col).struct.rename_fields(
                    [
                        f"{col}{seperator}{field_name}"
                        for field_name in df[col].struct.fields
                    ]
                )
                for col in struct_columns
            ]
        ).unnest(struct_columns)
        
    struct_columns = [col for col in df.columns if df[col].dtype == pl.Struct()]
    while len(struct_columns):
        df = _unnest_all(struct_columns=struct_columns)
        struct_columns = [col for col in df.columns if df[col].dtype == pl.Struct()]
        
    return df

@asset
def battle_data(season_rankings) -> None:
    '''Data on recent battles that players have participated in'''
    tbl = None
    for player_tag in season_rankings:
        formatted_tag = player_tag.replace('#', '%23')
        player_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}/battlelog'
        response = requests.get(player_url, headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pl.DataFrame(data)
                df = unnest_all(unnest_all(df.explode(['team', 'opponent'])).explode('team_cards').explode('opponent_cards'))
                if tbl is None:
                    tbl = df
                else:
                    tbl = pl.concat([tbl, df], how='diagonal_relaxed')
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    con.execute('CREATE OR REPLACE TABLE stg_battle_data AS SELECT * FROM tbl')

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

hitpoints = how much HP is left on tower at end of game
'''