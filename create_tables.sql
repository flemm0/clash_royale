/* DuckDB SQL commands used to create table schemas for pipeline */

USE clash_royale;

CREATE SCHEMA IF NOT EXISTS 'staging';
CREATE SCHEMA IF NOT EXISTS 'raw';

CREATE TABLE IF NOT EXISTS staging.stg_top_players_by_season (
  tag VARCHAR,
  name VARCHAR,
  expLevel BIGINT,
  tophies BIGINT,
  rank BIGINT,
  clan STRUCT(tag VARCHAR, "name" VARCHAR, badgeId BIGINT),
  season_id VARCHAR,
  UNIQUE(tag, season_id)
);

CREATE OR REPLACE TABLE raw.player_battle_log (
  type VARCHAR,
  battleTime VARCHAR,
  isLadderTournament BOOLEAN,
  arena_id BIGINT,
  arena_name VARCHAR,
  gameMode_id BIGINT,
  gameMode_name VARCHAR,
  deckSelection VARCHAR,
  isHostedMatch BOOLEAN,
  leagueNumber BIGINT,
  team_tag VARCHAR,
  opponent_tag VARCHAR,
  team_name VARCHAR,
  opponent_name VARCHAR,
  team_startingTrophies BIGINT,
  opponent_startingTrophies BIGINT,
  team_trophyChange INTEGER,
  opponent_trophyChange INTEGER,
  team_crowns BIGINT,
  opponent_crowns BIGINT,
  team_kingTowerHitPoints BIGINT,
  opponent_kingTowerHitPoints BIGINT,
  team_princessTowersHitPoints BIGINT[],
  opponent_princessTowersHitPoints BIGINT[],
  team_clan_tag VARCHAR,
  opponent_clan_tag VARCHAR,
  team_clan_name VARCHAR,
  opponent_clan_name VARCHAR,
  team_clan_badgeId BIGINT,
  opponent_clan_badgeId BIGINT,
  team_cards STRUCT("name" VARCHAR, "level" BIGINT, "elixirCost" INT)[],
  opponent_cards STRUCT("name" VARCHAR, "level" BIGINT, "elixirCost" INT)[],
  team_support_cards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, maxLevel BIGINT, rarity VARCHAR, iconUrls STRUCT(medium VARCHAR))[],
  opponent_support_cards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, maxLevel BIGINT, rarity VARCHAR, iconUrls STRUCT(medium VARCHAR))[],
  team_globalRank INT,
  opponent_globalRank INT,
  team_elixirLeaked DOUBLE,
  opponent_elixirLeaked DOUBLE,
  UNIQUE(team_tag, opponent_tag, battleTime)
);

CREATE TABLE IF NOT EXISTS staging.stg_player_stats (
  tag VARCHAR PRIMARY KEY,
  name VARCHAR,
  expLevel INT,
  trophies INT,
  bestTrophies INT,
  wins INT,
  losses INT,
  battleCount INT,
  threeCrownWins INT
);

CREATE TABLE IF NOT EXISTS staging.stg_clan_stats (
  tag VARCHAR PRIMARY KEY,
  "name" VARCHAR,
  "type" VARCHAR,
  description VARCHAR,
  clanScore INT,
  clanWarTrophies INT,
  requiredTrophies INT,
  members INT,
  location_id BIGINT,
  location_name VARCHAR,
  is_country BOOLEAN
);