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

def _this_month_cb() -> None:
    state.start_date = date(date.today().year, date.today().month, 1)
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
    st.button('This month', on_click=_this_month_cb)
    st.button('This year', on_click=_this_year_cb)
    st.button('All time', on_click=_all_time_cb)

    if state.start_date < min_dt:
        state.start_date = min_dt

    state.start_date, state.end_date = date_range_picker(
        "Date range",
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

    # TODO: create card method
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        with st.container(height=125, border=True):
            total_time = played["duration_ms"].sum() / 60000
            st.metric(
                label="Total time listened",
                value=f'{int(total_time)} min'
            )
    with c2:
        with st.container(height=125, border=True):
            st.metric(
                label="Different artists",
                value=played.drop_duplicates("main_artist_id").shape[0]
            )
    with c3:
        with st.container(height=125, border=True):
            st.metric(
                label="Different tracks",
                value=played.drop_duplicates("track_id").shape[0]
            )
    with c4:
        with st.container(height=125, border=True):
            avg_pop = played.drop_duplicates("track_id")["popularity"].mean()
            st.metric(
                label="Average popularity of tracks",
                value=round(avg_pop, 2),
                delta=round(avg_pop - track["popularity"].mean(), 2)
            )

    t1, t2, t3 = st.tabs(["Artists", "Tracks", "Genres"])

    with t1:
        st.header("Favorite Artists")

        # prepare spotlight container on top of the table
        spot1, spot2, spot3 = st.columns([1,2,2])

        artist_full = artist_played.merge(artist, how="left", left_on=["main_artist_id", "main_artist"], right_on=["id", "name"])
        event = st.dataframe(
            artist_full,
            column_config={
                "image": st.column_config.ImageColumn(""),
                "main_artist_id": None,
                "id": None,
                "duration_ms": None,
                "name": st.column_config.Column(""),
                "main_artist": None,
                "duration_min": st.column_config.NumberColumn("⏳", format="%d min", help="Time listened in minutes"),
                "followers": st.column_config.Column("👥", help="Followers"),
                "genres": st.column_config.Column("🎶", help="Genres"),
                "popularity": st.column_config.ProgressColumn("🌟", format="%f", min_value=0, max_value=100, help="Popularity"),
                "uri": None,
                "images": None
            },
            column_order=["image", "name", "duration_min", "genres", "popularity", "followers"],
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row"
        )

        if event.selection.rows:
            selected_idx = event.selection.rows[0]
        else:
            selected_idx = 0

        selected_id = artist_full["id"].iloc[selected_idx]
        selected_artist = artist_full[artist_full["id"] == selected_id].to_dict("records")[0]


        with spot1:
            with st.container(height=260, border=True):
                st.caption(f'#{selected_idx + 1} - {selected_artist["name"]}')
                st.image(selected_artist["image"], use_column_width=True)


        with spot2:
            with st.container(height=260, border=True):
                st.caption("Cumulative Time Listened (min)")
                selected_cum_time = played[played["main_artist_id"] == selected_id]
                selected_cum_time["duration_min"] = selected_cum_time["duration_ms"] / 60000
                selected_cum_time["duration_min_cumsum"] = selected_cum_time["duration_min"].cumsum()

                st.altair_chart(alt.Chart(selected_cum_time).mark_line().encode(
                    x=alt.X("monthdate(played_at)", title=None),
                    y=alt.Y("duration_min_cumsum", axis=alt.Axis(title=None, tickMinStep=1)),
                ).properties(height=180),
                use_container_width=True)

        with spot3:
            selected_artist_tracks = played[played["main_artist_id"] == selected_id].groupby(["image", "track"]).size().reset_index(name="count").sort_values(by="count", ascending=False)
            st.dataframe(
                selected_artist_tracks,
                column_config={
                    "image": st.column_config.ImageColumn("Cover"),
                    "track": st.column_config.Column("Title"),
                    "count": st.column_config.Column("Plays"),
                },
                column_order=["count", "image", "track"],
                hide_index=True,
                use_container_width=True,
                height=260)

        st.header("Artist Metrics")

        with st.container(border=True):
            selected_metric = st.selectbox("Select Metric", artist[["popularity", "followers"]].columns)

            # lookup dict for the limits of the selected metric
            limits = {
                "popularity": [0, 100],
                "followers": [artist["followers"].min(), artist["followers"].max()]
                }

            c1, c2 = st.columns([2, 3])
            with c1:
                selector = alt.selection_point(encodings=['x'])
                event = st.altair_chart(
                    alt.Chart(artist).mark_bar().encode(
                        x=alt.X(f"{selected_metric}:Q", bin=True, scale=alt.Scale(domain=limits[selected_metric])),
                        y='count(*):Q',
                        color=alt.condition(selector, f'{selected_metric}:Q', alt.value('lightgray'), legend=None, sort="descending")
                        ).add_selection(selector).properties(height=300),
                        use_container_width=True,
                        on_select="rerun"
                )
                # TODO: follower histogram should be log scale
            
            with c2:
                if not event["selection"]["param_1"]:
                    range_selection = limits[selected_metric]
                else:
                    range_selection = event["selection"]["param_1"][0][selected_metric]

                artist_param = artist[(artist[selected_metric] > range_selection[0]) & (artist[selected_metric] <= range_selection[1])].sort_values(by=selected_metric, ascending=False)

                st.dataframe(
                    artist_param,
                    column_config={
                        "image": st.column_config.ImageColumn("Cover"),
                        f"{selected_metric}": st.column_config.Column(f'{selected_metric} ({range_selection[0]} - {range_selection[1]})'),
                        "name": st.column_config.Column("Artist"),
                        "id": None,
                        "duration_ms": None,
                        "album_id": None,
                        "album_images": None,
                        "uri": None,
                    },
                    hide_index=True,
                    use_container_width=True,
                    column_order=[selected_metric, "image", "name"],
                    height=300
                    )

    with t2:
        st.header("Most Played Tracks")
        top_played = played
        top_played = top_played.groupby(["image", "track_id", "track", "artist","album", "popularity", "spotify_uri"]).size().reset_index(name="count").sort_values(by="count", ascending=False)

        # prepare spotlight container on top of the table
        spot1, spot2, spot3 = st.columns([1,2,2])

        event = st.dataframe(
            top_played,
            column_config={
                "image": st.column_config.ImageColumn(""),
                "track_id": None,
                "track": st.column_config.Column("Title"),
                "artist": st.column_config.Column("Artist"),
                "album": st.column_config.Column("Album"),
                "popularity": st.column_config.ProgressColumn("🌟", format="%f", min_value=0, max_value=100, help="Popularity"),
                "count": st.column_config.Column("Plays", help="Number of times this track was played"),
                "spotify_uri": st.column_config.LinkColumn("▶️", help="Open in Spotify", display_text="▶️"),
            },
            column_order=["image", "count", "track", "artist", "album", "popularity", "spotify_uri"],
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row"
        )

        if event.selection.rows:
            selected_idx = event.selection.rows[0]
        else:
            selected_idx = 0

        selected_id = top_played["track_id"].iloc[selected_idx]
        selected_track = top_played[top_played["track_id"] == selected_id].to_dict("records")[0]
        selected_af = audio_features[audio_features["track_id"] == selected_id]


        with spot1:
            with st.container(height=282, border=True):
                st.caption(f'#{selected_idx + 1} - {selected_track["track"]}')
                st.image(selected_track["image"], use_column_width=True)

        with spot2:
            with st.container(height=282, border=True):
                st.caption("Cumulative Plays")
                selected_cum_plays = played[played["track_id"] == selected_id].groupby("played_at").size().cumsum().reset_index(name="cumsum")

                st.altair_chart(alt.Chart(selected_cum_plays).mark_line().encode(
                    x=alt.X("monthdate(played_at)", title=None),
                    y=alt.Y("cumsum", axis=alt.Axis(title=None, tickMinStep=1)),
                ).properties(height=200), use_container_width=True)


        with spot3:
            selected_af_pivoted = pd.melt(selected_af, id_vars="track_id", value_vars=["acousticness", "danceability", "energy", "instrumentalness", "liveness", "speechiness", "valence"], var_name="Feature")

            st.dataframe(
                selected_af_pivoted,
                column_config={
                    "track_id": None,
                    "Feature": st.column_config.Column("Audio Feature"),
                    "value": st.column_config.ProgressColumn("Value", format="%.2f", min_value=0, max_value=1)
                },
                hide_index=True,
                use_container_width=True,
                height=282
            )

        st.header("Track Metrics")

        with st.container(border=True):
            # merge all played tracks with audio features
            p_select = played[["track_id", "track", "artist", "popularity", "image"]].drop_duplicates()
            track_full = p_select.merge(audio_features, on="track_id")

            selected_metric = st.selectbox("Select Metric", track_full.columns.drop(["track_id", "track", "artist", "image", "mode", "analysis_url"]))

            # lookup dict for the limits of the selected metric
            limits = {
                "popularity": [0, 100],
                "danceability": [0, 1],
                "energy": [0, 1],
                "key": [track_full["key"].min(), track_full["key"].max()],
                "loudness": [track_full["loudness"].min(), track_full["loudness"].max()],
                "speechiness": [0, 1],
                "acousticness": [0, 1],
                "instrumentalness": [0, 1],
                "liveness": [0, 1],
                "valence": [0, 1],
                "tempo": [track_full["tempo"].min(), track_full["tempo"].max()],
                "time_signature": [track_full["time_signature"].min(), track_full["time_signature"].max()]
                }

            c1, c2 = st.columns([2, 3])
            with c1:
                selector = alt.selection_point(encodings=['x'])
                event = st.altair_chart(
                    alt.Chart(track_full).mark_bar().encode(
                        x=alt.X(f"{selected_metric}:Q", bin=True, scale=alt.Scale(domain=limits[selected_metric])),
                        y='count(*):Q',
                        color=alt.condition(selector, f'{selected_metric}:Q', alt.value('lightgray'), legend=None, sort="descending")
                        ).add_selection(selector).properties(height=300),
                        use_container_width=True,
                        on_select="rerun"
                )
            
            with c2:
                if not event["selection"]["param_1"]:
                    range_selection = limits[selected_metric]
                else:
                    range_selection = event["selection"]["param_1"][0][selected_metric]
                    
                track_param = track_full[(track_full[selected_metric] > range_selection[0]) & (track_full[selected_metric] <= range_selection[1])].sort_values(by=selected_metric, ascending=False)

                st.dataframe(
                    track_param,
                    column_config={
                        "image": st.column_config.ImageColumn("Cover"),
                        f"{selected_metric}": st.column_config.Column(f'{selected_metric} ({range_selection[0]} - {range_selection[1]})'),
                        "track": st.column_config.Column("Title"),
                        "artist": st.column_config.Column("Artist"),
                        "id": None,
                        "duration_ms": None,
                        "album_id": None,
                        "album_images": None,
                        "uri": None,
                    },
                    hide_index=True,
                    use_container_width=True,
                    column_order=[selected_metric, "image", "track", "artist"],
                    height=300
                    )
                
    with t3:
        st.info("Genre statistics coming soon")