from dagster import AssetSelection, define_asset_job

daily_fact_table_update = define_asset_job(
        name='daily_fact_table_update',
        selection=['player_battle_log', 'fct_battle', 'player_stats*']
)

monthly_season_update = define_asset_job(
    name='monthly_season_update',
    selection=AssetSelection.all()
)