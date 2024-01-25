WITH columns_renamed AS (
    SELECT
        name AS card_name,
        id AS card_id,
        elixir AS elixir_cost,
        "type" AS card_type,
        rarity,
        arena AS arena_available,
        "description",
        evolved_spells_sc_key,
        is_evolved
    FROM {{ source('staging', 'stg_cards') }}
)

SELECT * FROM columns_renamed