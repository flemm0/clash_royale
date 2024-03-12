WITH columns_renamed AS (
    SELECT
        hash(tag) AS clan_id,
        tag AS clan_tag,
        "name" AS clan_name,
        "type" AS clan_type,
        "description",
        clanScore AS clan_score,
        clanWarTrophies AS clan_war_trophies,
        requiredTrophies AS required_trophies,
        members AS member_count,
        location_id,
        location_name,
        is_country
    FROM {{ source('staging', 'stg_clan_stats') }}
)

SELECT * FROM columns_renamed