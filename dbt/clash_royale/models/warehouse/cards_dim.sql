WITH clash_royale_cards AS (
    SELECT
        id,
        name,
        maxLevel AS max_level,
        maxEvolutionLevel AS max_evolution_level,
        elixirCost AS elixir_cost,
        iconUrls AS icon_urls
    FROM {{ source('clash_royale', 'stg_cards') }}
)

SELECT * FROM clash_royale_cards