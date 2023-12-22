# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import api, ladder_battles

from .resources import database_resource

all_assets = load_assets_from_modules([ladder_battles, api])

defs = Definitions(
    assets=[*all_assets],
    resources={
        'database': database_resource
    }
)