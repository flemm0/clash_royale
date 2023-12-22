import requests
import os
from dotenv import load_dotenv
import json
import duckdb
import polars as pl

from dagster import asset, Definitions, RetryPolicy, OpExecutionContext
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

@asset(retry_policy=RetryPolicy(max_retries=3, delay=30))
def player_data(context: OpExecutionContext, season_rankings: list) -> None:
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
    insert_counter = 0
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
            insert_counter += 1
            if insert_counter % 100 == 0:
                context.log.info(f'Inserted {insert_counter} results.')

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
def battle_data(context: OpExecutionContext, season_rankings: list) -> None:
    '''Data on recent battles that players have participated in'''
    con = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    cols = [
        'type', 'battleTime', 'isLadderTournament', 'arena', 'gameMode', 'deckSelection',
        'team', 'opponent', 'isHostedMatch', 'leagueNumber', 'challengeId', 
        'challengeWinCountBefore', 'challengeTitle'
    ]
    # check if table exists
    result = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='stg_battle_data'").fetchall()
    if len(result) == 0:
        create_table_query = '''
            CREATE TABLE stg_battle_data(
                type VARCHAR,
                battleTime VARCHAR,
                isLadderTournament BOOLEAN,
                arena STRUCT(id INT, name VARCHAR),
                gameMode STRUCT(id INT, name VARCHAR),
                deckSelection VARCHAR,
                team STRUCT(tag VARCHAR, "name" VARCHAR, crowns BIGINT, kingTowerHitPoints BIGINT, princessTowersHitPoints BIGINT[], clan STRUCT(tag VARCHAR, "name" VARCHAR, badgeId BIGINT), cards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, starLevel BIGINT, maxLevel BIGINT, elixirCost BIGINT, iconUrls STRUCT(medium VARCHAR, evolutionMedium VARCHAR), evolutionLevel BIGINT, maxEvolutionLevel BIGINT)[], globalRank JSON, elixirLeaked DOUBLE, trophyChange BIGINT, startingTrophies BIGINT)[],
                opponent STRUCT(tag VARCHAR, "name" VARCHAR, crowns BIGINT, kingTowerHitPoints BIGINT, princessTowersHitPoints BIGINT[], clan STRUCT(tag VARCHAR, "name" VARCHAR, badgeId BIGINT), cards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, starLevel BIGINT, maxLevel BIGINT, elixirCost BIGINT, iconUrls STRUCT(medium VARCHAR, evolutionMedium VARCHAR), evolutionLevel BIGINT, maxEvolutionLevel BIGINT)[], globalRank JSON, elixirLeaked DOUBLE, trophyChange BIGINT, startingTrophies BIGINT)[],
                isHostedMatch BOOLEAN,
                leagueNumber INTEGER,
                challengeId INTEGER,
                challengeWinCountBefore INTEGER,
                challengeTitle VARCHAR
            )
        '''
        con.execute(create_table_query)
    insert_counter = 0
    for player_tag in season_rankings:
        formatted_tag = player_tag.replace('#', '%23')
        player_url = f'https://api.clashroyale.com/v1/players/{formatted_tag}/battlelog'
        response = requests.get(player_url, headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                standardized_data = {col: data[col] if col in data else None for col in cols}
                df = pl.DataFrame(standardized_data)
                ## keep nested JSON as struct, and unnest in transformation stage
                #df = unnest_all(unnest_all(df.explode(['team', 'opponent'])).explode('team_cards').explode('opponent_cards'))
                con.execute('INSERT INTO stg_battle_data SELECT * FROM df')
                insert_counter += 1
                if insert_counter % 100 == 0:
                    context.log.info(f'Inserted battle data for {insert_counter} players.')


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

possible missing from battle data: team_trophyChange, team_cardsEvolutionLevel
'''