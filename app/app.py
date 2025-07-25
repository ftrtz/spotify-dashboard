from datetime import timedelta, date
import os

import streamlit as st
from streamlit_extras.mandatory_date_range import date_range_picker
from annotated_text import annotated_text
import altair as alt
import polars as pl

from load_tables import load_played_joined, load_artist, load_track, load_audio_features
from transform_tables import get_top_artists_played, get_top_tracks_played

# ========== STREAMLIT CONFIG
st.set_page_config(layout="wide")

# ========== DATABASE CONNECTION
db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_SECRET')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
db_schema = os.getenv("DB_SCHEMA")

# ========== LOAD DATA
played_raw = load_played_joined(db_url, db_schema)
artist = load_artist(db_url, db_schema)
track = load_track(db_url, db_schema)
audio_features = load_audio_features(db_url, db_schema)

# ========== DATE RANGES
min_dt = played_raw["played_at"].min().date()
max_dt = date.today() + timedelta(days=1)

state = st.session_state

# default start and end date when opening the application
if "start_date" not in state:
    state.start_date = date.today() - timedelta(days=14)

if "end_date" not in state:
    state.end_date = max_dt


# functions for date range buttons
def _today_cb() -> None:
    state.start_date = date.today()
    state.end_date = max_dt


def _last_seven_days_cb() -> None:
    state.start_date = date.today() - timedelta(days=7)
    state.end_date = max_dt


def _last_fourteen_days_cb() -> None:
    state.start_date = date.today() - timedelta(days=14)
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

    st.button("Today", on_click=_today_cb)
    st.button("Last 7 days", on_click=_last_seven_days_cb)
    st.button("Last 14 days", on_click=_last_fourteen_days_cb)
    st.button("This month", on_click=_this_month_cb)
    st.button("This year", on_click=_this_year_cb)
    st.button("All time", on_click=_all_time_cb)

    if state.start_date < min_dt:
        state.start_date = min_dt

    state.start_date, state.end_date = date_range_picker(
        "Date range",
        default_start=state.start_date,
        default_end=state.end_date,
        min_date=min_dt,
        max_date=max_dt,
    )

    start_date = state.start_date
    end_date = state.end_date

# Filter played data for the applied date range
if start_date and end_date:
    played = played_raw.filter(pl.col("played_at").is_between(start_date, end_date))

if played.shape[0] == 0:
    st.info("No data for the selected date range.")
else:
    # retrieve played data for artists and add artist information
    top_artists_played = get_top_artists_played(
        played, artist.with_columns(pl.col("genres").list.join(", "))
    )

    # ---------------------------------------- OVERALL STATS CARDS ----------------------------------------
    # --- Prepare genres
    all_genres = (
        top_artists_played.select("genres")
        .with_columns(pl.col("genres").str.split(", "))
        .explode("genres")
        .drop_nulls()
        .group_by("genres")
        .len("count")
        .sort("count", descending=True)
    )
    genres_count = all_genres.shape[0]

    st.title("Spotify Dashboard")

    # --- Overall stat cards
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        with st.container(height=125, border=True):
            total_time = played["duration_ms"].sum() / 60000
            st.metric(label="Total time listened", value=f"{int(total_time)} min")
    with c2:
        with st.container(height=125, border=True):
            st.metric(
                label="Different artists",
                value=played.unique("main_artist_id").shape[0],
            )
    with c3:
        with st.container(height=125, border=True):
            st.metric(label="Different genres", value=genres_count)
    with c4:
        with st.container(height=125, border=True):
            st.metric(
                label="Different tracks", value=played.unique("track_id").shape[0]
            )
    with c5:
        with st.container(height=125, border=True):
            avg_pop = played.unique("track_id")["popularity"].mean()
            st.metric(
                label="Average popularity of tracks",
                value=round(avg_pop, 2),
                delta=round(avg_pop - track["popularity"].mean(), 2),
            )

    t1, t2, t3 = st.tabs(["Favorites", "Metrics", "Recently Played"])

    # ---------------------------------------- TAB 1: FAVORITES ----------------------------------------
    with t1:
        # --- Artists
        st.header("Most Heard Artists")

        # prepare spotlight container on top of the table
        spot1, spot2, spot3 = st.columns([1, 2, 2])

        event = st.dataframe(
            top_artists_played.with_columns(pl.col("genres").str.split(", ")),
            column_config={
                "rank": st.column_config.NumberColumn("🔢", format="#%d", width=15),
                "image": st.column_config.ImageColumn(""),
                "main_artist_id": None,
                "id": None,
                "duration_ms": None,
                "name": st.column_config.Column(""),
                "main_artist": None,
                "duration_min": st.column_config.NumberColumn(
                    "⏳", format="%d min", help="Time listened in minutes"
                ),
                "followers": st.column_config.Column("👥", help="Followers"),
                "genres": st.column_config.Column("🎶", help="Genres"),
                "popularity": st.column_config.ProgressColumn(
                    "🌟", format="%f", min_value=0, max_value=100, help="Popularity"
                ),
                "uri": None,
                "images": None,
            },
            column_order=[
                "rank",
                "image",
                "name",
                "duration_min",
                "genres",
                "popularity",
                "followers",
            ],
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        if event.selection.rows:
            selected_idx = event.selection.rows[0]
        else:
            selected_idx = 0

        selected_artist = top_artists_played.row(selected_idx, named=True)

        # --- Selected Artist Spotlight
        with spot1:
            # Image
            annotated_text(
                (f"#{selected_artist['rank']}", f"{selected_artist['name']}")
            )

            st.markdown(
                f"""
                <div style="
                    width: 100%;
                    min-height: 218px;
                    background-image: url('{selected_artist["image"]}');
                    background-size: cover;
                    background-position: center;
                    border-radius: 10px;
                ">
                </div>
                """,
                unsafe_allow_html=True,
            )

        with spot2:
            # Cumulative Time Listened
            with st.container(height=260, border=True):
                st.caption("Cumulative Time Listened (min)")
                selected_cum_time = (
                    played.filter(
                        pl.col("main_artist_id") == selected_artist["main_artist_id"]
                    )
                    .with_columns(
                        [
                            (pl.col("duration_ms") / 60000).alias("duration_min"),
                        ]
                    )
                    .with_columns(
                        [pl.col("duration_min").cum_sum().alias("duration_min_cumsum")]
                    )
                )

                st.altair_chart(
                    alt.Chart(selected_cum_time)
                    .mark_line()
                    .encode(
                        x=alt.X("played_at", title=None),
                        y=alt.Y(
                            "duration_min_cumsum",
                            axis=alt.Axis(title=None, tickMinStep=1),
                        ),
                    )
                    .properties(height=180),
                    use_container_width=True,
                )

        with spot3:
            # Most Played Tracks
            selected_artist_tracks = (
                played.filter(
                    pl.col("main_artist_id") == selected_artist["main_artist_id"]
                )
                .group_by("image", "track")
                .len("count")
                .sort(by="count", descending=True)
            )
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
                height=260,
            )

        # --- Tracks
        st.header("Most Played Tracks")

        # prepare spotlight container on top of the table
        spot1, spot2, spot3 = st.columns([1, 2, 2])

        # prepare the track data for the table
        top_tracks_played = get_top_tracks_played(played)

        event = st.dataframe(
            top_tracks_played,
            column_config={
                "rank": st.column_config.NumberColumn("🔢", format="#%d", width=15),
                "image": st.column_config.ImageColumn(""),
                "track_id": None,
                "track": st.column_config.Column("Title"),
                "artist": st.column_config.Column("Artist"),
                "album": st.column_config.Column("Album"),
                "popularity": st.column_config.ProgressColumn(
                    "🌟", format="%f", min_value=0, max_value=100, help="Popularity"
                ),
                "count": st.column_config.Column(
                    "Plays", help="Number of times this track was played"
                ),
                "spotify_uri": st.column_config.LinkColumn(
                    "▶️", help="Open in Spotify", display_text="▶️"
                ),
            },
            column_order=[
                "rank",
                "image",
                "count",
                "track",
                "artist",
                "album",
                "popularity",
                "spotify_uri",
            ],
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        if event.selection.rows:
            selected_idx = event.selection.rows[0]
        else:
            selected_idx = 0

        selected_track = top_tracks_played.row(selected_idx, named=True)

        # --- Selected Track Spotlight
        with spot1:
            # Image
            annotated_text((f"#{selected_track['rank']}", f"{selected_track['track']}"))

            st.markdown(
                f"""
                <div style="
                    width: 100%;
                    min-height: 218px;
                    background-image: url('{selected_track["image"]}');
                    background-size: cover;
                    background-position: center;
                    border-radius: 10px;
                ">
                </div>
                """,
                unsafe_allow_html=True,
            )

        with spot2:
            # Cumulative Plays
            with st.container(height=282, border=True):
                st.caption("Cumulative Plays")

                selected_cum_plays = (
                    played.filter(pl.col("track_id") == selected_track["track_id"])
                    .sort("played_at")
                    .group_by("played_at")
                    .len("count")
                    .with_columns([pl.col("count").cum_sum().alias("cumsum")])
                )

                st.altair_chart(
                    alt.Chart(selected_cum_plays)
                    .mark_line()
                    .encode(
                        x=alt.X("played_at", title=None),
                        y=alt.Y("cumsum", axis=alt.Axis(title=None, tickMinStep=1)),
                    )
                    .properties(height=200),
                    use_container_width=True,
                )

        with spot3:
            # Audio Features
            selected_af = audio_features.filter(
                pl.col("track_id") == selected_track["track_id"]
            )
            if selected_af.shape[0] == 0:
                st.info(
                    "Audio Features are deprecated and aren't retrieved since November 2024."
                )

            else:
                selected_af_pivoted = selected_af.unpivot(
                    index="track_id",
                    on=[
                        "acousticness",
                        "danceability",
                        "energy",
                        "instrumentalness",
                        "liveness",
                        "speechiness",
                        "valence",
                    ],
                    variable_name="Feature",
                )

                st.dataframe(
                    selected_af_pivoted,
                    column_config={
                        "track_id": None,
                        "Feature": st.column_config.Column("Audio Feature"),
                        "value": st.column_config.ProgressColumn(
                            "Value", format="%.2f", min_value=0, max_value=1
                        ),
                    },
                    hide_index=True,
                    use_container_width=True,
                    height=282,
                )

        # --- Genres
        st.header("Most Popular Genres")

        # Extract top 10 genres as list of tuples
        top_genres = (
            all_genres.select(["genres", "count"]).head(10).iter_rows(named=False)
        )

        # Build the converted list for annotated_text
        top_genres_converted = []
        for genre, count in top_genres:
            top_genres_converted.append((genre, f"{count}x"))
            top_genres_converted.append(" ")

        annotated_text(top_genres_converted)

    # ---------------------------------------- TAB 2: METRICS ----------------------------------------
    with t2:
        st.header("Artist Metrics")

        with st.container(border=True):
            # filter for artists played in time window
            artist_filtered = artist.join(
                played, how="left", left_on="id", right_on="main_artist_id"
            )

            # Create selectbox with metrics
            selected_metric = st.selectbox(
                "Select Metric", artist_filtered[["popularity", "followers"]].columns
            )

            # lookup dict for the limits of the selected metric
            limits = {
                "popularity": [0, 100],
                "followers": [
                    artist_filtered["followers"].min(),
                    artist_filtered["followers"].max(),
                ],
            }

            c1, c2 = st.columns([2, 3])
            with c1:
                selector = alt.selection_point(encodings=["x"])
                event = st.altair_chart(
                    alt.Chart(artist_filtered)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            f"{selected_metric}:Q",
                            bin=True,
                            scale=alt.Scale(domain=limits[selected_metric]),
                        ),
                        y="count(*):Q",
                        color=alt.condition(
                            selector,
                            f"{selected_metric}:Q",
                            alt.value("lightgray"),
                            legend=None,
                            sort="descending",
                        ),
                    )
                    .add_params(selector)
                    .properties(height=300),
                    use_container_width=True,
                    on_select="rerun",
                )
                # TODO: follower histogram should be log scale

            with c2:
                if not event["selection"]["param_1"]:
                    range_selection = limits[selected_metric]
                else:
                    range_selection = event["selection"]["param_1"][0][selected_metric]

                artist_param = artist_filtered.filter(
                    (pl.col(selected_metric) > range_selection[0])
                    & (pl.col(selected_metric) <= range_selection[1])
                ).sort(selected_metric, descending=True)

                st.dataframe(
                    artist_param,
                    column_config={
                        "image": st.column_config.ImageColumn("Cover"),
                        f"{selected_metric}": st.column_config.Column(
                            f"{selected_metric} ({range_selection[0]} - {range_selection[1]})"
                        ),
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
                    height=300,
                )

        st.header(
            "Track Metrics",
            help="Audio Features were deprecated by Spotify. Only Tracks played before November 2024 have all Metrics.",
        )

        with st.container(border=True):
            # merge all played tracks with audio features
            p_select = played.select(
                "track_id", "track", "artist", "popularity", "image"
            ).unique()
            track_full = p_select.join(audio_features, on="track_id", how="left")

            selected_metric = st.selectbox(
                "Select Metric",
                track_full.drop(
                    "track_id", "track", "artist", "image", "mode", "analysis_url"
                ).columns,
            )

            # lookup dict for the limits of the selected metric
            limits = {
                "popularity": [0, 100],
                "danceability": [0, 1],
                "energy": [0, 1],
                "key": [track_full["key"].min(), track_full["key"].max()],
                "loudness": [
                    track_full["loudness"].min(),
                    track_full["loudness"].max(),
                ],
                "speechiness": [0, 1],
                "acousticness": [0, 1],
                "instrumentalness": [0, 1],
                "liveness": [0, 1],
                "valence": [0, 1],
                "tempo": [track_full["tempo"].min(), track_full["tempo"].max()],
                "time_signature": [
                    track_full["time_signature"].min(),
                    track_full["time_signature"].max(),
                ],
            }

            c1, c2 = st.columns([2, 3])
            with c1:
                selector = alt.selection_point(encodings=["x"])
                event = st.altair_chart(
                    alt.Chart(track_full)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            f"{selected_metric}:Q",
                            bin=True,
                            scale=alt.Scale(domain=limits[selected_metric]),
                        ),
                        y="count(*):Q",
                        color=alt.condition(
                            selector,
                            f"{selected_metric}:Q",
                            alt.value("lightgray"),
                            legend=None,
                            sort="descending",
                        ),
                    )
                    .add_params(selector)
                    .properties(height=300),
                    use_container_width=True,
                    on_select="rerun",
                )

            with c2:
                if not event["selection"]["param_1"]:
                    range_selection = limits[selected_metric]
                else:
                    range_selection = event["selection"]["param_1"][0][selected_metric]

                track_param = track_full.filter(
                    (pl.col(selected_metric) > range_selection[0])
                    & (pl.col(selected_metric) <= range_selection[1])
                ).sort(selected_metric, descending=True)

                st.dataframe(
                    track_param,
                    column_config={
                        "image": st.column_config.ImageColumn("Cover"),
                        f"{selected_metric}": st.column_config.Column(
                            f"{selected_metric} ({range_selection[0]} - {range_selection[1]})"
                        ),
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
                    height=300,
                )

    with t3:
        st.header("Recently Played Tracks")

        recently_played = played.sort(by="played_at", descending=True)

        st.dataframe(
            recently_played,
            column_config={
                "played_at": st.column_config.DatetimeColumn("Played at"),
                "image": st.column_config.ImageColumn(""),
                "track_id": None,
                "track": st.column_config.Column("Title"),
                "artist": st.column_config.Column("Artist"),
                "album": st.column_config.Column("Album"),
                "popularity": st.column_config.ProgressColumn(
                    "🌟", format="%f", min_value=0, max_value=100, help="Popularity"
                ),
                "spotify_uri": st.column_config.LinkColumn(
                    "▶️", help="Open in Spotify", display_text="▶️"
                ),
            },
            column_order=[
                "played_at",
                "image",
                "track",
                "artist",
                "album",
                "popularity",
                "spotify_uri",
            ],
            hide_index=True,
            use_container_width=True,
        )
