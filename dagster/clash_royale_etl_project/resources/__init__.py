from dagster_duckdb import DuckDBResource
from dagster import EnvVar

motherduck_token = EnvVar('MOTHERDUCK_TOKEN')

database_resource = DuckDBResource(
    database=f'md:clash_royale?motherduck_token={motherduck_token}'
)