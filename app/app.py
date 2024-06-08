import streamlit as st
from sqlalchemy import create_engine
import os
import pandas as pd


# ========== LAYOUT
# Use the full page instead of a narrow central column
st.set_page_config(layout="wide")
c1, c2, c3 = st.columns((1, 3, 1))

with c2:
    main_container = st.container()
    filter_container = st.container()

# ========== LOAD DATA
# Prepare connection to postgresql database
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_SECRET = os.getenv("POSTGRES_SECRET")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_SECRET}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# join played table
query = """
        select
            played_at,
            track.id as track_id,
            track.name as track,
            track.popularity as popularity,
            string_agg(artist.name, ', ') as artist,
            track.album_name as album,
            track.uri as spotify_uri,
            track.album_images->0->'url'->>0 as image
        from played
        join track on played.track_id = track.id
        join track_artist on track.id = track_artist.track_id
        join artist on track_artist.artist_id = artist.id
        group by played_at, track.id
        order by played_at;
        """

df = pd.read_sql_query(query, con=engine)





# ========== CONTENT

with c2:
# --- FILTER WINDOW
    with filter_container:
        # Slider
        min_dt = df["played_at"].min().to_pydatetime()
        max_dt = df["played_at"].max().to_pydatetime()

        datetime_range = st.slider(
            "Datetime Range",
            min_value=min_dt,
            max_value=max_dt,
            value=(min_dt, max_dt),
            format="YYYY/MM/DD")

# --- MAIN WINDOW
    with main_container:
        st.title("Most Played Spotify Tracks")

        # --- APPLY FILTERS
        if datetime_range:
            df = df[df["played_at"].between(datetime_range[0], datetime_range[1])]
        
        # show top 10 most played
        top_played = df
        top_played["count"] = top_played.groupby("track_id").transform("size")
        top_played = top_played[["image", "track", "artist", "album", "popularity", "count", "spotify_uri"]].drop_duplicates().sort_values(by="count", ascending=False).head(10)
        top_played = top_played.rename(columns={
            "track": "Title",
            "artist": "Artist",
            "album": "Album",
            "count": "Plays"
            })

        st.dataframe(
            top_played,
            column_config={
                "image": st.column_config.ImageColumn("Cover"),
                "spotify_uri": st.column_config.LinkColumn("Spotify Link", display_text="Play"),
                "popularity": st.column_config.ProgressColumn("Popularity", format="%f", min_value=0, max_value=100)
            },
            hide_index=True)

