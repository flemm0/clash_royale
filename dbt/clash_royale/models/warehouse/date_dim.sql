{{ config(materialized='table') }}

WITH date_parts AS (
    SELECT
        battleTime,
        YEAR(battleTime) AS year,
        MONTH(battleTime) AS month,
        DAY(battleTime) AS day,
        EXTRACT('hour' FROM battleTime) AS hour,
        EXTRACT('minute' FROM battleTime) AS minute,
        EXTRACT('second' FROM battleTime) AS second
    FROM {{ source('clash_royale', 'raw_battles_12072020_01042021') }}
)

SELECT * FROM date_parts