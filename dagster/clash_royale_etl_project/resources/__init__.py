from dagster_duckdb import DuckDBResource
import os
from dotenv import load_dotenv

load_dotenv()

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')

database_resource = DuckDBResource(
    database=f'md:clash_royale?motherduck_token={motherduck_token}'
)