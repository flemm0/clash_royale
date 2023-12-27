-- every player must play with a deck of eight unique cards
select
    battle_id,
    player_tag,
    count(card_id) as card_count
from {{ ref('battle_cards_bridge') }}
group by 1, 2
having card_count <> 8