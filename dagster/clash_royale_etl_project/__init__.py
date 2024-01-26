# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import players, cards, clans, seasons, dbt_assets, constants

from .resources import database_resource

from dagster_dbt import DbtCliResource

import os


python_assets = load_assets_from_modules([cards, clans, seasons, players])
dbt_assets = load_assets_from_modules([dbt_assets])

defs = Definitions(
    assets=[*python_assets, *dbt_assets],
    resources={
        'database': database_resource,
        'dbt': DbtCliResource(project_dir=os.fspath(constants.dbt_project_dir))
    }
)