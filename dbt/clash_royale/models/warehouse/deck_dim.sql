WITH deck_extracted AS (
  SELECT 
    team_cards AS deck,
    [
      team_cards[1].name, team_cards[2].name, team_cards[3].name, team_cards[4].name, 
      team_cards[5].name, team_cards[6].name, team_cards[7].name, team_cards[8].name,
    ] 	AS cards_list
  FROM {{ source('raw', 'player_battle_log') }}
  UNION ALL
  SELECT 
    opponent_cards AS deck,
    [
      opponent_cards[1].name, opponent_cards[2].name, opponent_cards[3].name, opponent_cards[4].name, 
      opponent_cards[5].name, opponent_cards[6].name, opponent_cards[7].name, opponent_cards[8].name,
    ] 	AS cards_list
  FROM {{ source('raw', 'player_battle_log') }}
),
deck_id_added AS (
  SELECT
    hash(cards_list) AS deck_id, 
    *
  FROM deck_extracted
), 
card_type_counts_added AS (
  SELECT
    *,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE rarity = 'Common')) AS common_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE rarity = 'Rare')) AS rare_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE rarity = 'Epic')) AS epic_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE rarity = 'Legendary')) AS legendary_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE rarity = 'Champion')) AS champion_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE type = 'Troop')) AS troop_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE type = 'Spell')) AS spell_count,
    (SELECT COUNT(*) FROM UNNEST(cards_list) AS t(element) WHERE element IN (SELECT "name" FROM {{ source('staging', 'stg_cards') }} WHERE type = 'Building')) AS building_count,
  FROM
    deck_id_added
),
average_elixir_cost_calculated AS (
  SELECT
    * EXCLUDE(deck),
    list_avg([
      deck[1].elixirCost, deck[2].elixirCost, deck[3].elixirCost, deck[4].elixirCost, 
      deck[5].elixirCost, deck[6].elixirCost, deck[7].elixirCost, deck[8].elixirCost,
    ]) AS average_elixir_cost
  FROM card_type_counts_added
)
SELECT * FROM average_elixir_cost_calculated