# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import players, cards, clans, seasons, dbt_assets, constants

from .resources import database_resource

from .schedules import fact_table_update_schedule

from .jobs import daily_fact_table_update

from dagster_dbt import DbtCliResource

import os

python_assets = load_assets_from_modules([cards, clans, seasons, players])
dbt_assets = load_assets_from_modules([dbt_assets])

all_jobs = [daily_fact_table_update]

all_schedules = [fact_table_update_schedule]

defs = Definitions(
    assets=[*python_assets, *dbt_assets],
    resources={
        'database': database_resource,
        'dbt': DbtCliResource(project_dir=os.fspath(constants.dbt_project_dir))
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
