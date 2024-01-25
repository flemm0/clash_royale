WITH columns_renamed AS (
    SELECT
        hash(tag) AS player_id,
        tag AS player_tag,
        "name" AS player_name,
        expLevel AS experience_level,
        trophies AS trophy_count,
        bestTrophies AS best_trophy_count,
        wins,
        losses,
        battleCount AS battle_count,
        threeCrownWins AS three_crown_wins
    FROM {{ source('staging', 'stg_player_stats') }}
),

additional_fields_added AS (
    SELECT
        *,
        three_crown_wins / wins AS three_crown_win_percentage,
        wins / losses AS win_loss_ratio
    FROM columns_renamed
)

SELECT * FROM additional_fields_added