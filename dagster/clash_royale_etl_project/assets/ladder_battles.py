from . import constants
from dagster import asset
from dagster_duckdb import DuckDBResource, OpExecutionContext
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import shutil
import polars as pl
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import re
import requests
import duckdb
from datetime import datetime

api = KaggleApi()
api.authenticate()

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}


@asset
def raw_battle_data() -> None:
    '''Raw player vs. player data sourced from Kaggle'''
    dataset_id = 'bwandowando/clash-royale-season-18-dec-0320-dataset'
    api.dataset_download_files(dataset_id, unzip=True, path=constants.RAW_DATA_PATH)
    for root, dir, files in os.walk(constants.RAW_DATA_PATH):
        for file in files:
            src_path = os.path.join(root, file)
            dest_path = os.path.join(constants.RAW_DATA_PATH, file)
            shutil.move(src_path, dest_path)
    # Remove empty directories
    for root, dirs, files in os.walk(constants.RAW_DATA_PATH, topdown=False):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):  # Check if directory is empty
                os.rmdir(dir_path)  # Remove empty directory

@asset(
    deps=['raw_battle_data']
)
def raw_parquet_battle_data() -> None:
    '''Raw player vs. player data converted into Parquet file format'''
    if not os.path.exists(constants.PARQUET_DATA_PATH):
        os.mkdir(constants.PARQUET_DATA_PATH)
        
    for file in os.listdir(constants.RAW_DATA_PATH):
        if file.endswith('WL_tagged.csv'):
            src_file = os.path.join(constants.RAW_DATA_PATH, file)
            dest_file = os.path.join(constants.PARQUET_DATA_PATH, os.path.splitext(file)[0] + '.parquet')
            writer = None
            with csv.open_csv(src_file) as reader:
                for next_chunk in reader:
                    if next_chunk is None:
                        break
                    if writer is None:
                        writer = pq.ParquetWriter(dest_file, next_chunk.schema)
                    next_table = pa.Table.from_batches([next_chunk])
                    writer.write_table(next_table)
            writer.close()
    # clean up raw csv directory to save space
    for file in os.listdir(constants.RAW_DATA_PATH):
        path = os.path.join(constants.RAW_DATA_PATH, file)
        try:
            if os.path.isfile(path):
                os.remove(path)
        except Exception as e:
            print(f'Failed to delete {path}: {e}')

def extract_date(dir) -> str:
    '''Extracts dates from directory of dated parquet files. Used for creating raw table name with range of dates.'''
    dates = []
    for file in os.listdir(dir):
        dates.append([datetime.strptime(date, '%m%d%Y') for date in re.findall(r'\d+', file)])
    dates = [date for sublist in dates for date in (sublist if isinstance(sublist, list) else [sublist])]
    return f"{datetime.strftime(min(dates), '%m%d%Y')}_{datetime.strftime(max(dates), '%m%d%Y')}"

@asset(
    deps=['raw_parquet_battle_data'],
    metadata={'schema': 'raw'}
)
def raw_battle_data_table(database: DuckDBResource) -> None:
    '''Upload raw player vs. player data into Motherduck table.'''
    table_name = f'raw_battles_{extract_date(constants.PARQUET_DATA_PATH)}'
    query = f'''
        CREATE OR REPLACE TABLE raw.{table_name} 
        AS (
            SELECT * FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        );
    '''
    with database.get_connection() as conn:
        conn.execute(query=query)

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