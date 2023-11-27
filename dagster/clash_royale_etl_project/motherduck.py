import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

motherduck_token = os.getenv('MOTHERDUCK_KEY')

con = duckdb.connect(f'md:?motherduck_token={motherduck_token}')

con.sql('SHOW DATABASES').show()