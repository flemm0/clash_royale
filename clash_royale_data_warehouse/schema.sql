CREATE SCHEMA IF NOT EXISTS main;
CREATE SCHEMA raw;
CREATE SCHEMA staging;



CREATE TABLE player_dim(player_id UBIGINT, player_tag VARCHAR, player_name VARCHAR, experience_level INTEGER, trophy_count INTEGER, best_trophy_count INTEGER, wins INTEGER, losses INTEGER, battle_count INTEGER, three_crown_wins INTEGER, three_crown_win_percentage DOUBLE, win_loss_ratio DOUBLE);
CREATE TABLE deck_dim(deck_id UBIGINT, cards_list VARCHAR[], common_count BIGINT, rare_count BIGINT, epic_count BIGINT, legendary_count BIGINT, champion_count BIGINT, troop_count BIGINT, spell_count BIGINT, building_count BIGINT, average_elixir_cost DOUBLE);
CREATE TABLE date_dim(date_id VARCHAR, battle_date DATE, "year" BIGINT, "month" BIGINT, month_name VARCHAR, "day" BIGINT, day_name VARCHAR, is_weekend VARCHAR, month_day_year VARCHAR);
CREATE TABLE fct_battle(battle_id VARCHAR, date_id VARCHAR, player_id UBIGINT, deck_id UBIGINT, clan_id UBIGINT, battle_type VARCHAR, is_ladder_tournament BOOLEAN, arena_id BIGINT, arena_name VARCHAR, game_mode_id BIGINT, game_mode_name VARCHAR, deck_selection_method VARCHAR, is_hosted_match BOOLEAN, league_number BIGINT, starting_trophies BIGINT, trophy_change INTEGER, crowns_earned BIGINT, king_tower_hit_points BIGINT, princess_tower_hit_points BIGINT[], cards STRUCT("name" VARCHAR, "level" BIGINT, elixirCost INTEGER)[], support_cards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, maxLevel BIGINT, rarity VARCHAR, iconUrls STRUCT(medium VARCHAR))[], global_rank INTEGER, elixir_leaked DOUBLE);
CREATE TABLE clan_dim(clan_id UBIGINT, clan_tag VARCHAR, clan_name VARCHAR, clan_type VARCHAR, description VARCHAR, clan_score INTEGER, clan_war_trophies INTEGER, required_trophies INTEGER, member_count INTEGER, location_id BIGINT, location_name VARCHAR, is_country BOOLEAN);
CREATE TABLE card_dim(card_name VARCHAR, card_id BIGINT, elixir_cost BIGINT, card_type VARCHAR, rarity VARCHAR, arena_available BIGINT, description VARCHAR, evolved_spells_sc_key VARCHAR, is_evolved BOOLEAN);
CREATE TABLE raw.player_battle_log("type" VARCHAR, battleTime VARCHAR, isLadderTournament BOOLEAN, arena_id BIGINT, arena_name VARCHAR, gameMode_id BIGINT, gameMode_name VARCHAR, deckSelection VARCHAR, isHostedMatch BOOLEAN, leagueNumber BIGINT, team_tag VARCHAR, opponent_tag VARCHAR, team_name VARCHAR, opponent_name VARCHAR, team_startingTrophies BIGINT, opponent_startingTrophies BIGINT, team_trophyChange INTEGER, opponent_trophyChange INTEGER, team_crowns BIGINT, opponent_crowns BIGINT, team_kingTowerHitPoints BIGINT, opponent_kingTowerHitPoints BIGINT, team_princessTowersHitPoints BIGINT[], opponent_princessTowersHitPoints BIGINT[], team_clan_tag VARCHAR, opponent_clan_tag VARCHAR, team_clan_name VARCHAR, opponent_clan_name VARCHAR, team_clan_badgeId BIGINT, opponent_clan_badgeId BIGINT, team_cards STRUCT("name" VARCHAR, "level" BIGINT, elixirCost INTEGER)[], opponent_cards STRUCT("name" VARCHAR, "level" BIGINT, elixirCost INTEGER)[], team_supportCards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, maxLevel BIGINT, rarity VARCHAR, iconUrls STRUCT(medium VARCHAR))[], opponent_supportCards STRUCT("name" VARCHAR, id BIGINT, "level" BIGINT, maxLevel BIGINT, rarity VARCHAR, iconUrls STRUCT(medium VARCHAR))[], team_globalRank INTEGER, opponent_globalRank INTEGER, team_elixirLeaked DOUBLE, opponent_elixirLeaked DOUBLE, UNIQUE(team_tag, opponent_tag, battleTime));
CREATE TABLE staging.stg_top_players_by_season(tag VARCHAR, "name" VARCHAR, expLevel BIGINT, tophies BIGINT, rank BIGINT, clan STRUCT(tag VARCHAR, "name" VARCHAR, badgeId BIGINT), season_id VARCHAR, UNIQUE(tag, season_id));
CREATE TABLE staging.stg_seasons(id VARCHAR, query_url VARCHAR);
CREATE TABLE staging.stg_cards("key" VARCHAR, "name" VARCHAR, sc_key VARCHAR, elixir BIGINT, "type" VARCHAR, rarity VARCHAR, arena BIGINT, description VARCHAR, id BIGINT, evolved_spells_sc_key VARCHAR, is_evolved BOOLEAN);
CREATE TABLE staging.stg_player_stats(tag VARCHAR PRIMARY KEY, "name" VARCHAR, expLevel INTEGER, trophies INTEGER, bestTrophies INTEGER, wins INTEGER, losses INTEGER, battleCount INTEGER, threeCrownWins INTEGER);
CREATE TABLE staging.stg_clan_stats(tag VARCHAR PRIMARY KEY, "name" VARCHAR, "type" VARCHAR, description VARCHAR, clanScore INTEGER, clanWarTrophies INTEGER, requiredTrophies INTEGER, members INTEGER, location_id BIGINT, location_name VARCHAR, is_country BOOLEAN);




