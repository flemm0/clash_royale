# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import ladder_battles, players, cards, clans, seasons

from .resources import database_resource

old_assets = load_assets_from_modules([ladder_battles, cards, clans])
new_assets = load_assets_from_modules([seasons, players])

defs = Definitions(
    assets=[*old_assets, *new_assets],
    resources={
        'database': database_resource
    }
)