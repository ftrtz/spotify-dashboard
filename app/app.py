import streamlit as st
from streamlit_extras.mandatory_date_range import date_range_picker
from streamlit_extras.add_vertical_space import add_vertical_space
import altair as alt
from datetime import timedelta, date
from sqlalchemy import create_engine
import os
import pandas as pd

# for local dev
#from dotenv import load_dotenv
#load_dotenv()

# ========== STREAMLIT CONFIG
# Use the full page instead of a narrow central column
st.set_page_config(layout="wide")


# ========= FUNCTIONS
# loading data from db
def get_played_joined(engine):
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

def get_artist(engine):
    query = f"""
        select *, images->1->'url'->>0 as image
        from artist;
        """
    return pd.read_sql_query(query, con=engine)

def get_track(engine):
    query = f"""
        select *, album_images->1->'url'->>0 as image
        from track;
        """
    return pd.read_sql_query(query, con=engine)

# transforming data
def get_artist_played(played):
    artist_played = played.groupby(["main_artist_id", "main_artist"]).agg({"duration_ms": "sum"}).reset_index().sort_values("duration_ms", ascending=False)
    artist_played["duration_min"] = artist_played["duration_ms"] / 60000
    return artist_played


# ========== DATABASE CONNECTION
# Prepare connection to postgresql database
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

# ========== CONTENT

# --- SET FILTERS IN SIDEBAR
# Datetime Slider
min_dt = played_raw["played_at"].min().to_pydatetime().date()
max_dt = played_raw["played_at"].max().to_pydatetime().date() + timedelta(days=1)

# Initialize session state for date ranges
state = st.session_state

if 'start_date' not in state:
    state.start_date = min_dt

if 'end_date' not in state:
    state.end_date = max_dt

# Update session state based on button clicks
def _today_cb():
    state.start_date = date.today()
    state.end_date = max_dt

def _last_week_cb():
    state.start_date = date.today() - timedelta(days=7)
    state.end_date = max_dt

def _this_year_cb():
    state.start_date = date(date.today().year, 1, 1)
    state.end_date = max_dt

def _all_time_cb():
    state.start_date = min_dt
    state.end_date = max_dt


with st.sidebar:
    st.header("Date filter")

    st.button('Today', on_click=_today_cb)
    st.button('Last week', on_click=_last_week_cb)
    st.button('This year', on_click=_this_year_cb)
    st.button('All time', on_click=_all_time_cb)

    # Ensure start_date does not exceed min_dt
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

# --- APPLY FILTERS
if start_date and end_date:
    played = played_raw[played_raw["played_at"].between(start_date, end_date)]


# --- TRANSFORM DATA FOR VIZ
artist_played = get_artist_played(played)

# --- MAIN
st.title("Spotify Dashboard")

c1, c2, c3 = st.columns([2, 6, 1])
with c1:
    add_vertical_space(6)
    #st.header("General")

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
    
    st.metric(
        label="Number of different genres",
        value="Placeholder"
        )
    
    
with c2:
    t1, t2 = st.tabs(["Artists", "Tracks"])

    with t1:
        st.header("Favorite Artist")

        # Prepare favourite artist data
        fav_artist_id = artist_played["main_artist_id"].values[0]
        fav_artist = artist[artist["id"] == fav_artist_id].to_dict("records")[0]
        fav_artist_playtime = int(artist_played["duration_min"].values[0])

        # get the most played track for fav artist
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
            y=alt.Y("duration_min",title="Time listened (min)"),
            ).configure_axis(grid=False),
            use_container_width=True
        )

    with t2:
        st.header("Most Played Tracks")

        # show top 10 most played
        top_played = played
        top_played = top_played.groupby(["image", "track_id", "track", "artist", "popularity", "spotify_uri"]).size().reset_index(name="count").sort_values(by="count", ascending=False).head(10)
        top_played = top_played.drop(columns="track_id")
        top_played = top_played.rename(columns={
            "track": "Title",
            "artist": "Artist",
            "count": "Plays"
            })
        

        st.dataframe(
            top_played,
            column_config={
                "image": st.column_config.ImageColumn("Cover"),
                "Artist": st.column_config.Column("Artist", width="medium"),
                "spotify_uri": st.column_config.LinkColumn("Spotify", display_text="Play"),
                "popularity": st.column_config.ProgressColumn("Popularity", format="%f", min_value=0, max_value=100)
            },
            hide_index=True
            )
        
        st.subheader("Selected track Details (audio features, play only there?, etc)")
        st.subheader("Time listened")
        st.subheader("Number of Different Tracks")
