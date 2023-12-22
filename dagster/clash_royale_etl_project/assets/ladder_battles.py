from . import constants
from dagster import asset
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import shutil
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import duckdb

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

@asset(
    deps=['raw_parquet_battle_data']
)
def raw_battle_data_table():
    '''Upload raw player vs. player data into Motherduck table'''
    conn = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
    query = f'''
        CREATE OR REPLACE TABLE raw_battles_12072020_01032021 
        AS (
            SELECT * FROM "{constants.PARQUET_DATA_PATH}/*.parquet"
        );
    '''
    conn.execute(query=query)