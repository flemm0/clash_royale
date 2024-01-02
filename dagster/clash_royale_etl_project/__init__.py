# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import ladder_battles, players, cards, clans

from .resources import database_resource

all_assets = load_assets_from_modules([ladder_battles, players, cards, clans])

defs = Definitions(
    assets=[*all_assets],
    resources={
        'database': database_resource
    }
)