{{ config(materialized='table') }}

WITH date_parts AS (
    SELECT
        battleTime,
        YEAR(battleTime) AS `year`,
        MONTH(battleTime) AS `month`,
        DAY(battleTime) AS `day`,
        TIME(battleTime) AS `time`
    FROM {{ ref('raw_battles_12072020_01032021') }}
),
date_dimension AS (
    SELECT
        battleTime as `date`,
        year,
        month,
        day,
        `time`,
    FROM date_parts
)

SELECT *
FROM date_dimension