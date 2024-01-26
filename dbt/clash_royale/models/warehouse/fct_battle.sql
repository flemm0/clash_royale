WITH team_table_with_keys_added AS(
  SELECT 
    md5(CONCAT(battleTime, team_tag, opponent_tag)) AS battle_id,
    strftime(date_trunc('day', strptime(battleTime, '%Y%m%dT%H%M%S.%fZ')),'%Y%m%d') AS date_id,
    hash(team_tag) AS player_id,
    hash([
      team_cards[1].name, team_cards[2].name, team_cards[3].name, team_cards[4].name, 
      team_cards[5].name, team_cards[6].name, team_cards[7].name, team_cards[8].name,
    ]) AS deck_id,
  hash(team_clan_tag) AS clan_id,
  "type" AS battle_type,
  isLadderTournament AS is_ladder_tournament,
  arena_id,
  arena_name,
  gameMode_id AS game_mode_id,
  replace(gameMode_name, '_', ' ') AS game_mode_name,
  deckSelection AS deck_selection_method,
  isHostedMatch AS is_hosted_match,
  leagueNumber AS league_number,
  team_startingTrophies AS starting_trophies,
  team_trophyChange AS trophy_change,
  team_crowns AS crowns_earned,
  team_kingTowerHitPoints AS king_tower_hit_points,
  team_princessTowersHitPoints AS princess_tower_hit_points,
  team_cards AS cards,
  team_supportCards AS support_cards,
  team_globalRank AS global_rank,
  team_elixirLeaked AS elixir_leaked
  FROM {{ source('raw', 'player_battle_log') }}
),

opponent_table_with_keys_added AS (
  SELECT 
    md5(CONCAT(battleTime, team_tag, opponent_tag)) AS battle_id,
    strftime(date_trunc('day', strptime(battleTime, '%Y%m%dT%H%M%S.%fZ')),'%Y%m%d') AS date_id,
    hash(opponent_tag) AS player_id,
    hash([
      opponent_cards[1].name, opponent_cards[2].name, opponent_cards[3].name, opponent_cards[4].name, 
      opponent_cards[5].name, opponent_cards[6].name, opponent_cards[7].name, opponent_cards[8].name,
    ]) AS deck_id,
  hash(opponent_clan_tag) AS clan_id,
  "type" AS battle_type,
  isLadderTournament AS is_ladder_tournament,
  arena_id,
  arena_name,
  gameMode_id AS game_mode_id,
  replace(gameMode_name, '_', ' ') AS game_mode_name,
  deckSelection AS deck_selection_method,
  isHostedMatch AS is_hosted_match,
  leagueNumber AS league_number,
  opponent_startingTrophies AS starting_trophies,
  opponent_trophyChange AS trophy_change,
  opponent_crowns AS crowns_earned,
  opponent_kingTowerHitPoints AS king_tower_hit_points,
  opponent_princessTowersHitPoints AS princess_tower_hit_points,
  opponent_cards AS cards,
  opponent_supportCards AS support_cards,
  opponent_globalRank AS global_rank,
  opponent_elixirLeaked AS elixir_leaked
  FROM {{ source('raw', 'player_battle_log') }}
),

team_and_opponent_tables_combined AS (
SELECT * FROM team_table_with_keys_added
UNION 
SELECT * FROM opponent_table_with_keys_added
)

SELECT * FROM team_and_opponent_tables_combined