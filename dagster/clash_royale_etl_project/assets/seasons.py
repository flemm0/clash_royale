import requests
import os
import polars as pl

from dagster import asset
from dagster_duckdb import DuckDBResource

api_key = os.getenv('API_TOKEN')
headers = {'Authorization': f'Bearer {api_key}'}

@asset(
    metadata={'schema': 'staging', 'table': 'staging.stg_seasons'}
)
def list_of_seasons(database: DuckDBResource) -> None:
    seasons_url = 'https://api.clashroyale.com/v1/locations/global/seasons'
    response = requests.get(seasons_url, headers=headers)
    if response.status_code == 200:
        data = response.json()['items']
        if data:
            df = pl.DataFrame(data=data)
            df = df.with_columns(
                pl.when(pl.col('id') > '2022-09')
                .then(pl.col('id').map_elements(lambda season_id: f'https://api.clashroyale.com/v1/locations/global/pathoflegend/{season_id}/rankings/players/'))
                .otherwise(pl.col('id').map_elements(lambda season_id: f'https://api.clashroyale.com/v1/locations/global/seasons/{season_id}/rankings/players/'))
                .alias('query_url')
            ).unique(maintain_order=True)
            with database.get_connection() as conn:
                conn.execute("CREATE OR REPLACE TABLE staging.stg_seasons AS SELECT * FROM df")