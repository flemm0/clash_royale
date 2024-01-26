import streamlit as st
from utils import *

st.title('Clash Royale Analytics Dashboard :100:')
st.subheader('Made by Flemming Wu', divider='rainbow')

with st.container(border=True):
    st.header('Card Information')
    body = '''
    For those unfamiliar with Clash Royale, the following table shows the cards available for users to have in their battle decks.\n
    This table includes each card's name, cost during battle, the type (troop, building or spell), its rarity, a description, and an image of the card!
    Double click the image to see it bigger.
    '''
    st.markdown(body)
    df = get_card_info()
    st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={
            'card_name': 'Card Name',
            'card_id': None,
            'elixir_cost': st.column_config.Column('Elixir Cost', width='small'),
            'card_type': 'Card Type',
            'rarity': 'Rarity',
            'arena_available': 'Arena Available',
            'description': st.column_config.Column('Description', width='large'),
            'is_evolved': None,
            'evolved_spells_sc_key': None,
            'url': st.column_config.ImageColumn('Preview Image', help='Double click on image to view larger')
        }
    )

with st.container(border=True):
    st.header('Common Deck Usage For Past 90 Days')
    body = '''
    The following table shows how frequently a deck (combination of 8 cards) was used in the past 90 days.
    The total usage in battle is totaled in the "Total Usage" column, and there is a line chart depicting the usage trend over time.
    '''
    st.markdown(body=body)

    df = deck_usage_past_thirty_days()
    st.data_editor(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={
            'cards_list': st.column_config.Column('Deck', width='large'),
            'concatenated': st.column_config.LineChartColumn(
                'Usage (past 90 days)',
                width='large',
                help='number of times deck used in past 30 days',
            ),
            'total_usage': st.column_config.Column('Total Usage', width='small')
        }
    )

with st.container(border=True):
    rarity_tab, type_tab = st.tabs(['Card Usage by Rarity', 'Card Usage by Type'])

    with rarity_tab:
        st.header('Most Common Cards by Deck Usage Colored by Rarity')
        body = '''
        The bar chart below shows how often a card was selected to use in battle, colored by the *rarity* of the card.
        '''
        st.markdown(body=body)
        fig = most_common_cards(grouping='rarity')
        st.plotly_chart(fig, use_container_width=True)

    with type_tab:
        st.header('Most Common Cards by Deck Usage Colored by Type')
        body = '''
        The bar chart below shows how often a card was selected to use in battle, colored by the *type* of the card.
        '''
        st.markdown(body=body)
        fig = most_common_cards(grouping='card_type')
        st.plotly_chart(fig, use_container_width=True)

with st.container(border=True):
    st.header('Average Deck\'s Elixir Cost by Arena')
    body = '''
    As players play the game more, they move their way up the different arenas. 
    More experienced players play in higher arenas.
    The chart below shows how much the average battle deck costs in elixir for each arena.
    '''
    st.markdown(body=body)
    fig = elixir_cost_by_arena()
    st.plotly_chart(fig, use_container_width=True)

with st.container(border=True):
    st.header('Card Elixir Cost vs Usage in Battle')
    body = '''
    Cumulative battle deck appearances for each card, split by the cards elixir cost. 
    Hover over smaller bars to read labels.
    '''
    st.markdown(body=body)
    fig = card_appearances_by_elixir_cost()
    st.plotly_chart(fig, use_container_width=True)