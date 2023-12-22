from . import constants
from dagster import asset
from dagster_duckdb import DuckDBResource
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import shutil
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import re
from datetime import datetime

os.environ['KAGGLE_CONFIG_DIR'] = constants.KAGGLE_CONFIG_DIR

api = KaggleApi()
api.authenticate()

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')


@asset
def raw_battle_data():
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
def raw_parquet_battle_data():
    '''Raw player vs. player data converted into Parquet file format'''
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

def extract_date(dir):
    '''Extracts dates from directory of dated parquet files. Used for creating raw table name with range of dates.'''
    dates = []
    for file in os.listdir(dir):
        dates.append([datetime.strptime(date, '%m%d%Y') for date in re.findall(r'\d+', file)])
    dates = [date for sublist in dates for date in (sublist if isinstance(sublist, list) else [sublist])]
    return f"{datetime.strftime(min(dates), '%m%d%Y')}_{datetime.strftime(max(dates), '%m%d%Y')}"

@asset(
    deps=['raw_parquet_battle_data']
)
def raw_battle_data_table(database: DuckDBResource):
    '''Upload raw player vs. player data into Motherduck table.'''
    table_name = f'raw_battles_{extract_date(constants.PARQUET_DATA_PATH)}'
    query = f'''
        CREATE OR REPLACE TABLE {table_name} 
        AS (
            SELECT * FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        );
    '''
    with database.get_connection() as conn:
        conn.execute(query=query)