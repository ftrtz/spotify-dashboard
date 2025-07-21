import connectorx as cx
from polars import DataFrame
import streamlit as st


def load_played_joined(_db_url: str, db_schema: str) -> DataFrame:
    """
    Load data from the database and join tables to get played tracks with their details.

    Parameters:
    - _db_url (str): Database Connection URL

    Returns:
    - polars.DataFrame: DataFrame with played track details.
    """
    query = f"""
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
        from {db_schema}.played
        join {db_schema}.track on played.track_id = track.id
        join {db_schema}.track_artist on track.id = track_artist.track_id
        join {db_schema}.artist on track_artist.artist_id = artist.id
        group by played_at, track.id
        order by played_at
    """

    df = cx.read_sql(conn=_db_url, query=query, return_type="polars")

    return df


@st.cache_data(ttl=3600)
def load_artist(_db_url: str, db_schema: str) -> DataFrame:
    """
    Load artist data from the database.

    Parameters:
    - _db_url (str): Database Connection URL

    Returns:
    - polars.DataFrame: DataFrame with artist details.
    """
    query = f"""
        select *, images->1->'url'->>0 as image
        from {db_schema}.artist
    """
    df = cx.read_sql(conn=_db_url, query=query, return_type="polars")

    return df


@st.cache_data(ttl=3600)
def load_track(_db_url: str, db_schema: str) -> DataFrame:
    """
    Load track data from the database.

    Parameters:
    - _db_url (str): Database Connection URL

    Returns:
    - polars.DataFrame: DataFrame with track details.
    """
    query = f"""
        select *, album_images->1->'url'->>0 as image
        from {db_schema}.track
    """
    df = cx.read_sql(conn=_db_url, query=query, return_type="polars")

    return df


@st.cache_data(ttl=3600)
def load_audio_features(_db_url: str, db_schema: str) -> DataFrame:
    """
    Load audio features data from the database.

    Parameters:
    - _db_url (str): Database Connection URL

    Returns:
    - polars.DataFrame: DataFrame with audio features details.
    """
    query = f"""
        select *
        from {db_schema}.audio_features
    """
    df = cx.read_sql(conn=_db_url, query=query, return_type="polars")

    return df
