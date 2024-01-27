import streamlit as st
import duckdb
import polars as pl
import requests
import plotly.express as px

st.set_page_config(layout="wide")

def init_connection():
    '''Function to establish connection to MotherDuck'''
    motherduck_token = st.secrets['MOTHERDUCK_TOKEN']
    return duckdb.connect(f'md:clash_royale?motherduck_token={motherduck_token}', read_only=True)

@st.cache_data
def cr_card_images() -> pl.DataFrame:
    '''Function to query data warehouse raw table for cards and their images'''
    with init_connection() as conn:
        df = conn.sql('SELECT * FROM raw.card_images_reference').pl()
    return df

def most_common_cards(grouping: str) -> px.bar:
    with init_connection() as conn:
        query = f'''
            WITH card_appearances AS (
                SELECT
                    card_name.cards_list AS card,
                    COUNT(*) AS deck_appearances
                FROM fct_battle
                JOIN
                    (
                        SELECT
                            deck_id,
                            card_name
                        FROM
                            deck_dim,
                            UNNEST(deck_dim.cards_list) AS card_name
                    ) AS unnested_cards ON fct_battle.deck_id = unnested_cards.deck_id
                GROUP BY card_name
                ORDER BY deck_appearances DESC
            )
            SELECT card_appearances.*, card_dim.{grouping}
            FROM card_appearances
            JOIN card_dim on card_appearances.card = card_dim.card_name
            ORDER BY deck_appearances DESC;
        '''
        df = conn.sql(query=query).pl()
    fig = px.bar(
        df, 
        x='card', 
        y='deck_appearances', 
        color=grouping,
        labels={'deck_appearances': 'Number of Deck Appearances', 'card': 'Card', grouping: grouping.replace('_', ' ').capitalize()}
    )
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    return fig
    
def elixir_cost_by_arena() -> px.bar:
    with init_connection() as conn:
        query = '''
            SELECT arena_name, AVG(average_elixir_cost) AS average_elixir_cost
            FROM (
            SELECT DISTINCT player_id, fct_battle.deck_id, arena_name, average_elixir_cost
            FROM fct_battle 
            JOIN
                (
                SELECT deck_id, average_elixir_cost
                FROM deck_dim
                ) 	AS deck_cost ON deck_cost.deck_id = fct_battle.deck_id
            WHERE arena_name IS NOT NULL
            )
            GROUP BY arena_name;
        '''
        df = conn.sql(query=query).pl()
    fig = px.bar(
        data_frame=df,
        x='arena_name',
        y='average_elixir_cost',
        color='arena_name',
        labels={'average_elixir_cost': 'Average Elixir Cost of Decks Used', 'arena_name': 'Arena Name'}
    )
    fig.update_xaxes(
        categoryorder='array', 
        categoryarray= [f'Arena {i}' for i in range(1, 24)] + ['Legendary Arena', 'Clan League']
    )
    fig.update_layout(showlegend=False)
    return fig

def deck_usage_past_thirty_days():
    query = f"""
        SELECT battle_date, cards_list, COUNT(*) AS usage
        FROM (
            SELECT DISTINCT battle_id, date_dim.battle_date, cards_list
            FROM fct_battle
            JOIN deck_dim ON fct_battle.deck_id = deck_dim.deck_id
            JOIN date_dim ON fct_battle.date_id = date_dim.date_id
            WHERE date_dim.battle_date > current_date() - INTERVAL 90 DAY
            AND fct_battle.deck_selection_method <> 'predefined'
        )
        GROUP BY 1, 2
        ORDER BY 1 ASC, 3 DESC
    """
    with init_connection() as conn:
        df = conn.execute(query=query).pl()
        df = conn.sql('PIVOT df ON battle_date USING SUM(usage) GROUP BY cards_list').pl()

    df = df.fill_null(0).to_pandas()
    df['concatenated'] = df.iloc[:,1:].apply(lambda row: row.to_list(), axis=1)
    df['total_usage'] = df['concatenated'].apply(sum)
    df = df.sort_values(by=['total_usage'], ascending=False)
    df = df[['cards_list', 'concatenated', 'total_usage']]
    return df

def get_card_info() -> pl.DataFrame:
    '''Queries data warehouse for card dimension table'''
    with init_connection() as conn:
        query = 'SELECT * FROM card_dim'
        df = conn.sql(query=query).pl()

    df = df.join(other=cr_card_images(), on='card_name')
    return df

def card_appearances_by_elixir_cost() -> px.scatter:
    query = '''
        WITH card_appearances AS (    
            SELECT
                card_name.cards_list AS card,
                COUNT(*) AS deck_appearances
            FROM fct_battle
            JOIN
                (
                    SELECT
                        deck_id,
                        card_name
                    FROM
                        deck_dim,
                        UNNEST(deck_dim.cards_list) AS card_name
                ) AS unnested_cards ON fct_battle.deck_id = unnested_cards.deck_id
            GROUP BY card_name
            ORDER BY deck_appearances DESC
        )
        SELECT card_appearances.*, elixir_cost
        FROM card_appearances
        JOIN card_dim ON card_appearances.card = card_dim.card_name
    '''
    with init_connection() as conn:
        df = conn.sql(query).pl()
    fig = px.bar(
        df,
        x='elixir_cost',
        y='deck_appearances',
        color='card',
        text='card',
        labels={
            'elixir_cost': 'Elixir Cost',
            'deck_appearances': 'Cumulative Deck Appearances'
        },
    )
    fig.update_layout(showlegend=False)
    return fig