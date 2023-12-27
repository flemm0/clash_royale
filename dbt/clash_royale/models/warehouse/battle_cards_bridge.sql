with winner_cards_long_format as ( 

  select 
    battle_id, 
    winner_tag as player_tag,
    unnest(winner_cards) as card_level
  from {{ ref('stg_battles') }}

), 

winner_cards_id_level_separated as (

  select
    battle_id, 
    player_tag, 
    map_keys(card_level)[1] as card_id, 
    map_values(card_level)[1] as card_level
  from winner_cards_long_format

),

loser_cards_long_format as (

    select
        battle_id,
        loser_tag as player_tag, 
        unnest(loser_cards) as card_level
    from {{ ref('stg_battles') }}

), 

loser_cards_id_level_separated as (

    select
        battle_id,
        player_tag,
        map_keys(card_level)[1] as card_id,
        map_values(card_level)[1] as card_level
    from loser_cards_long_format

)

select * from winner_cards_id_level_separated
union
select * from loser_cards_id_level_separated