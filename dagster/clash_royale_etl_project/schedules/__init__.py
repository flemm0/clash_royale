from dagster import ScheduleDefinition
from ..jobs import daily_fact_table_update

fact_table_update_schedule = ScheduleDefinition(
        job=daily_fact_table_update,
        cron_schedule="0 12 * * *" # every day at noon
)
