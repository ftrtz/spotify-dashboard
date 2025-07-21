from polars import DataFrame
import streamlit as st
import polars as pl


@st.cache_data(ttl=3600)
def get_top_artists_played(played: DataFrame, artist: DataFrame) -> DataFrame:
    """
    Compute top artists by total played duration and join with artist metadata.

    Groups the played data by artist, calculates total duration played in ms and minutes,
    sorts by duration descending, joins with artist metadata, and adds a rank column.

    Args:
        played (DataFrame): Polars DataFrame containing played track data,
            with at least 'main_artist_id', 'main_artist', and 'duration_ms' columns.
        artist (DataFrame): Polars DataFrame containing artist metadata,
            with at least 'id' column to join on 'main_artist_id'.

    Returns:
        DataFrame: Polars DataFrame with:
            - artist play totals (ms and min),
            - joined artist metadata,
            - rank column,
            sorted by total duration played descending.
    """
    top_artists_played = (
        played.group_by(["main_artist_id", "main_artist"])
        .agg(pl.col("duration_ms").sum().alias("duration_ms"))
        .sort("duration_ms", descending=True)
        .with_columns((pl.col("duration_ms") / 60000).alias("duration_min"))
    )

    top_artists_played = top_artists_played.join(
        artist, how="left", left_on="main_artist_id", right_on="id"
    )
    top_artists_played = top_artists_played.with_row_index(name="rank", offset=1)

    return top_artists_played


@st.cache_data(ttl=3600)
def get_top_tracks_played(played: DataFrame) -> DataFrame:
    """
    Compute top tracks by play count from played data.

    Groups the played data by track-related columns, counts the number of plays,
    sorts by play count descending, and adds a rank column.

    Args:
        played (DataFrame): Polars DataFrame containing played track data,
            with columns including 'image', 'track_id', 'track',
            'artist', 'album', 'popularity', and 'spotify_uri'.

    Returns:
        DataFrame: Polars DataFrame with:
            - track play counts,
            - rank column,
            sorted by play count descending.
    """
    top_tracks_played = (
        played.group_by(
            "image", "track_id", "track", "artist", "album", "popularity", "spotify_uri"
        )
        .len("count")
        .sort(by="count", descending=True)
        .with_row_index(name="rank", offset=1)
    )

    return top_tracks_played
