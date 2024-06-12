import streamlit as st
from streamlit_extras.mandatory_date_range import date_range_picker
import altair as alt
from datetime import timedelta, date

from sqlalchemy import create_engine
import os
import pandas as pd


# ========= HELPER FUNCTIONS


# ========== LAYOUT
# Use the full page instead of a narrow central column
#st.set_page_config(layout="wide")


# ========== LOAD DATA
# Prepare connection to postgresql database
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_SECRET = os.getenv("POSTGRES_SECRET")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_SECRET}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# join played table
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

df = pd.read_sql_query(query, con=engine, parse_dates="played_at")


# ========== CONTENT

# --- SET FILTERS
# Datetime Slider
min_dt = df["played_at"].min().to_pydatetime().date()
max_dt = df["played_at"].max().to_pydatetime().date() + timedelta(days=1)
start_date = min_dt
end_date = max_dt

# debug
min_dt = date.today() - timedelta(days=1000)
# debug

with st.sidebar:
    st.header("Date filter")
    if st.button("Today"):
        start_date = date.today()
        end_date = date.today() + timedelta(days=1)

    if st.button("Last week"):
        start_date = date.today() - timedelta(days=7)
        end_date = date.today() + timedelta(days=1)

    if st.button("This Year"):
        start_date = date(date.today().year, 1, 1)
        end_date = date.today() + timedelta(days=1)
    
    if start_date < min_dt: 
            start_date = min_dt

    date_range = date_range_picker(
        "Pick a date range",
        min_date=min_dt,
        max_date=max_dt,
        default_start=start_date,
        default_end=end_date,
    )
    datetime_range = [pd.to_datetime(x).tz_localize('UTC') for x in date_range]

# --- APPLY FILTERS
if datetime_range:
    df = df[df["played_at"].between(datetime_range[0], datetime_range[1])]

# --- MAIN
st.title("Spotify Stats")


t1, t2 = st.tabs(["Artists", "Tracks"])

with t1:
    st.header("Favorite Artist")
    top_artist = df
    top_artist = top_artist.groupby(["main_artist_id", "main_artist"]).agg({"duration_ms": "sum"}).reset_index().sort_values("duration_ms", ascending=False)
    top_artist["duration_min"] = top_artist["duration_ms"] / 60000

    # Fetch additional data for the favourite artist
    fav_artist_id = top_artist["main_artist_id"].values[0]
    query = f"""
        select *, images->1->'url'->>0 as image
        from artist
        where id = '{fav_artist_id}';
        """

    fav_artist = pd.read_sql_query(query, con=engine)

    fav_artist_name = fav_artist["name"].values[0]
    fav_artist_min = int(top_artist["duration_min"].values[0])

    # get the most played track for fav artist
    fav_artist_track = df[df["main_artist_id"] == fav_artist_id]
    fav_artist_track = fav_artist_track.groupby(["image", "track_id", "track"]).size().reset_index(name="count").sort_values(by="count", ascending=False)

    c1, c2 = st.columns(2)
    with c1:
        st.image(f'{fav_artist["image"].values[0]}')
    with c2:
        st.subheader(fav_artist_name)
        st.markdown(f'You spent **{fav_artist_min}** minutes with *{fav_artist_name}*')
        st.markdown(f':busts_in_silhouette: {fav_artist["followers"].values[0]}')
        genres = ", ".join(fav_artist["genres"].values[0])
        st.markdown(f':notes: {genres if genres else "No genres defined"}')
        st.progress(value=int(fav_artist["popularity"].values[0]), text=f'Popularity: {fav_artist["popularity"].values[0]}')
        st.markdown(f'Your most played *{fav_artist_name}* song is *{fav_artist_track["track"].values[0]}* (**{fav_artist_track["count"].values[0]}** times)')

    st.markdown(f'You listened to **{top_artist.shape[0]}** different artists. Here are your favourites:')
    st.altair_chart(alt.Chart(top_artist.head(20)).mark_bar().encode(
        x=alt.X("main_artist", sort=None, axis=alt.Axis(title=None)),
        y=alt.Y("duration_min",title="Time listened (min)"),
        ).configure_axis(grid=False),
        use_container_width=True
    )

with t2:
    st.header("Most Played Tracks")

    # show top 10 most played
    top_played = df
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
