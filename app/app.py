import streamlit as st
from streamlit_extras.mandatory_date_range import date_range_picker
from streamlit_extras.add_vertical_space import add_vertical_space
import altair as alt
from datetime import timedelta, date, datetime
from sqlalchemy import create_engine, Engine
import os
import pandas as pd
from pandas import DataFrame
from typing import Optional

# ========== STREAMLIT CONFIG
st.set_page_config(layout="wide")

# ========= FUNCTIONS
def get_played_joined(engine: Engine) -> DataFrame:
    """
    Load data from the database and join tables to get played tracks with their details.

    Parameters:
    - engine (sqlalchemy.Engine): Database engine.

    Returns:
    - pandas.DataFrame: DataFrame with played track details.
    """
    query = """
        select
            played_at,
            track.id as track_id,
            track.name as track,
            track.popularity as popularity,
            track.duration_ms as duration_ms,
            string_agg(artist.name, ', ' order by track_artist.artist_position) as artist,
            max(case when track_artist.artist_position = 0 then artist.id end) as main_artist_id,
            max(case when track_artist.artist_position = 0 then artist.name end) as main_artist,
            track.album_name as album,
            track.uri as spotify_uri,
            track.album_images->1->'url'->>0 as image
        from played
        join track on played.track_id = track.id
        join track_artist on track.id = track_artist.track_id
        join artist on track_artist.artist_id = artist.id
        group by played_at, track.id
        order by played_at;
    """
    return pd.read_sql_query(query, con=engine, parse_dates="played_at")

def get_artist(engine: Engine) -> DataFrame:
    """
    Load artist data from the database.

    Parameters:
    - engine (sqlalchemy.Engine): Database engine.

    Returns:
    - pandas.DataFrame: DataFrame with artist details.
    """
    query ="""
        select *, images->1->'url'->>0 as image
        from artist;
    """
    return pd.read_sql_query(query, con=engine)

def get_track(engine: Engine) -> DataFrame:
    """
    Load track data from the database.

    Parameters:
    - engine (sqlalchemy.Engine): Database engine.

    Returns:
    - pandas.DataFrame: DataFrame with track details.
    """
    query = """
        select *, album_images->1->'url'->>0 as image
        from track;
    """
    return pd.read_sql_query(query, con=engine)

def get_audio_features(engine: Engine) -> DataFrame:
    """
    Load audio features data from the database.

    Parameters:
    - engine (sqlalchemy.Engine): Database engine.

    Returns:
    - pandas.DataFrame: DataFrame with audio features details.
    """
    query = """
        select *
        from audio_features;
    """
    return pd.read_sql_query(query, con=engine)

def get_artist_played(played: DataFrame) -> DataFrame:
    """
    Transforms the played dataframe and calculates the duration in minutes for the main artist.

    Parameters:
    - played (pandas.DataFrame): DataFrame with data to be grouped and aggregated.

    Returns:
    - pandas.DataFrame: DataFrame with grouped and aggregated data, including "duration_min" column.
    """
    artist_played = played.groupby(["main_artist_id", "main_artist"]).agg({"duration_ms": "sum"}).reset_index().sort_values("duration_ms", ascending=False)
    artist_played["duration_min"] = artist_played["duration_ms"] / 60000
    return artist_played

# ========== DATABASE CONNECTION
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_SECRET = os.getenv("POSTGRES_SECRET")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_SECRET}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# ========== LOAD DATA
played_raw = get_played_joined(engine)
artist = get_artist(engine)
track = get_track(engine)
audio_features = get_audio_features(engine)

# ========== CONTENT
min_dt = played_raw["played_at"].min().to_pydatetime().date()
max_dt = date.today() + timedelta(days=1)

state = st.session_state

if 'start_date' not in state:
    state.start_date = min_dt

if 'end_date' not in state:
    state.end_date = max_dt

def _today_cb() -> None:
    state.start_date = date.today()
    state.end_date = max_dt

def _last_seven_days_cb() -> None:
    state.start_date = date.today() - timedelta(days=7)
    state.end_date = max_dt

def _this_year_cb() -> None:
    state.start_date = date(date.today().year, 1, 1)
    state.end_date = max_dt

def _all_time_cb() -> None:
    state.start_date = min_dt
    state.end_date = max_dt

with st.sidebar:
    st.header("Date filter")

    st.button('Today', on_click=_today_cb)
    st.button('Last 7 days', on_click=_last_seven_days_cb)
    st.button('This year', on_click=_this_year_cb)
    st.button('All time', on_click=_all_time_cb)

    if state.start_date < min_dt:
        state.start_date = min_dt

    state.start_date, state.end_date = date_range_picker(
        "Pick a custom date range",
        default_start=state.start_date,
        default_end=state.end_date,
        min_date=min_dt,
        max_date=max_dt
    )
    
    start_date = pd.to_datetime(state.start_date).tz_localize('UTC')
    end_date = pd.to_datetime(state.end_date).tz_localize('UTC')

if start_date and end_date:
    played = played_raw[played_raw["played_at"].between(start_date, end_date)]

if played.shape[0] == 0:
    st.info("No data for the selected date range.")
else:
    artist_played = get_artist_played(played)

    st.title("Spotify Dashboard")

    c1, c2, c3 = st.columns([2, 6, 1])
    with c1:
        add_vertical_space(6)

        total_time = played["duration_ms"].sum() / 60000
        st.metric(
            label="Total time listened",
            value=f'{int(total_time)} min'
        )
        
        st.metric(
            label="Different artists",
            value=played.drop_duplicates("main_artist_id").shape[0]
        )
        
        st.metric(
            label="Different tracks",
            value=played.drop_duplicates("track_id").shape[0]
        )
        
        avg_pop = played.drop_duplicates("track_id")["popularity"].mean()
        st.metric(
            label="Average popularity of tracks",
            value=round(avg_pop, 2),
            delta=round(avg_pop - track["popularity"].mean(), 2)
        )

    with c2:
        t1, t2 = st.tabs(["Artists", "Tracks"])

        with t1:
            st.header("Favorite Artist")

            fav_artist_id = artist_played["main_artist_id"].values[0]
            fav_artist = artist[artist["id"] == fav_artist_id].to_dict("records")[0]
            fav_artist_playtime = int(artist_played["duration_min"].values[0])

            fav_artist_track = played[played["main_artist_id"] == fav_artist_id]
            fav_artist_track = fav_artist_track.groupby(["image", "track_id", "track"]).size().reset_index(name="count").sort_values(by="count", ascending=False).to_dict("records")[0]

            m1, m2 = st.columns(2)
            with m1:
                st.image(f'{fav_artist["image"]}')
            with m2:
                st.subheader(fav_artist["name"])
                st.markdown(f'You spent **{fav_artist_playtime}** minutes with *{fav_artist["name"]}*')
                st.markdown(f':busts_in_silhouette: {fav_artist["followers"]}')
                genres = ", ".join(fav_artist["genres"])
                st.markdown(f':notes: {genres if genres else "No genres defined"}')
                st.progress(value=int(fav_artist["popularity"]), text=f'Popularity: {fav_artist["popularity"]}')
                st.markdown(f'Your most played *{fav_artist["name"]}* song is *{fav_artist_track["track"]}* (**{fav_artist_track["count"]}** times)')

            st.markdown(f'You listened to **{artist_played.shape[0]}** different artists. Here are your favourites:')
            st.altair_chart(alt.Chart(artist_played.head(20)).mark_bar().encode(
                x=alt.X("main_artist", sort=None, axis=alt.Axis(title=None, labelAngle=-60, labelOverlap=False)),
                y=alt.Y("duration_min", title="Time listened (min)"),
            ).configure_axis(grid=False), use_container_width=True)

        with t2:
            st.header("Most Played Tracks")

            top_played = played
            top_played = top_played.groupby(["image", "track_id", "track", "artist", "popularity", "spotify_uri"]).size().reset_index(name="count").sort_values(by="count", ascending=False).head(10)

            event = st.dataframe(
                top_played,
                column_config={
                    "image": st.column_config.ImageColumn("Cover"),
                    "track_id": None,
                    "track": st.column_config.Column("Title"),
                    "artist": st.column_config.Column("Artist", width="medium"),
                    "spotify_uri": None,
                    "popularity": st.column_config.ProgressColumn("Popularity", format="%f", min_value=0, max_value=100),
                    "count": st.column_config.Column("Plays"),
                },
                hide_index=True,
                use_container_width=True,
                on_select="rerun",
                selection_mode="single-row"
            )

            with st.expander("Details and Audio features", expanded=True):
                if not event.selection.rows:
                    st.info("Select track to show detailed audio features.")
                else:
                    selected_id = top_played["track_id"].iloc[event.selection.rows[0]]
                    selected_af = audio_features[audio_features["track_id"] == selected_id]

                    exp_c1, exp_c2 = st.columns([1, 2])

                    with exp_c1:
                        st.image(
                            image=f'{top_played[top_played["track_id"] == selected_id]["image"].values[0]}',
                            caption=f'{top_played[top_played["track_id"] == selected_id]["track"].values[0]} - {top_played[top_played["track_id"] == selected_id]["artist"].values[0]}'
                        )
                        st.link_button(":green[Open in Spotify]", url=f'{top_played[top_played["track_id"] == selected_id]["spotify_uri"].values[0]}', use_container_width=True)

                    with exp_c2:
                        selected_af_pivoted = pd.melt(selected_af, id_vars="track_id", value_vars=["acousticness", "danceability", "energy", "instrumentalness", "liveness", "speechiness", "valence"], var_name="Feature")

                        st.dataframe(
                            selected_af_pivoted,
                            column_config={
                                "track_id": None,
                                "Feature": st.column_config.Column("Feature", width="medium"),
                                "value": st.column_config.ProgressColumn("Value", width="medium", format="%.2f", min_value=0, max_value=1)
                            },
                            hide_index=True,
                            use_container_width=True
                        )

                        selected_cum_plays = played[played["track_id"] == selected_id].groupby("played_at").size().cumsum().reset_index(name="cumsum")
                        st.caption("Cumulative Plays")

                        st.altair_chart(alt.Chart(selected_cum_plays).mark_line().encode(
                            x=alt.X("monthdate(played_at)", title=None),
                            y=alt.Y("cumsum", axis=alt.Axis(title=None, tickMinStep=1)),
                        ).properties(height=150), use_container_width=True)
