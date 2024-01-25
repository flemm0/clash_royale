WITH datetime_type_converted AS (
   SELECT STRPTIME(battleTime, '%Y%m%dT%H%M%S.%fZ') AS battle_time
   FROM {{ source('raw', 'player_battle_log') }}
),
additional_queryable_fields_added AS (
  SELECT 
    date_trunc('day', battle_time) AS battle_date,
    year(battle_time) AS "year",
    month(battle_time) AS "month",
    monthname(battle_time) AS month_name,
    day(battle_time) AS "day",
    dayname(battle_time) AS day_name,
    CASE 
      WHEN dayname(battle_time) IN ('Saturday', 'Sunday') THEN 'yes'
      ELSE 'no'
    END AS is_weekend,
    strftime(battle_time, '%B %-d, %Y') month_day_year
    FROM datetime_type_converted
),
distinct_and_ordered_dates AS (
  SELECT DISTINCT *
  FROM additional_queryable_fields_added
  ORDER BY battle_date ASC
),
date_id_added AS (
  SELECT 
    strftime(battle_date, '%Y%m%d') AS date_id, *
  FROM distinct_and_ordered_dates
)
SELECT * FROM date_id_added