#%%
from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

api.download_file('bwandowando/clash-royale-season-18-dec-0320-dataset', 'BattlesStaging_01012021_WL_tagged.csv', path="data")
# %%
import duckdb
from pathlib import Path

raw_data_path = Path('/home/flemm0/clash_royale_etl_project/data/kaggle/raw')
for file in raw_data_path.iterdir():
    out_file = Path(str(file).replace('csv', 'parquet'))
    duckdb.sql(f'''
        LOAD PARQUET;
        COPY (SELECT * FROM read_csv_auto("{file}")) TO "output.parquet" (FORMAT PARQUET);
    ''')
# %%
import duckdb
import os
#from dagster import EnvVar

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
conn = duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}')
#%%
query = '''
    CREATE OR REPLACE TABLE raw_battles_12072020_01032021 
    AS (
        SELECT * FROM "../../../data/kaggle/raw/*.csv"
    );
'''
conn.execute(query=query)

