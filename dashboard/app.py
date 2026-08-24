import os

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv


load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


st.set_page_config(
    page_title="Reddit Streaming Analytics",
    layout="wide",
)

st.title("Streaming Analytics Dashboard")
st.caption(
    "Synthetic Reddit-like events processed through Kafka, Spark, and PostgreSQL. "
    "Dashboard refreshes every 3 seconds."
)


@st.fragment(run_every="3s")
def render_dashboard():
    with get_connection() as connection:
        recent_events = pd.read_sql(
            """
            SELECT
                event_timestamp,
                subreddit,
                text,
                text_length,
                text_size
            FROM reddit_events
            WHERE event_timestamp IS NOT NULL
            ORDER BY event_timestamp DESC
            LIMIT 20
            """,
            connection,
        )

        metrics = pd.read_sql(
            """
            SELECT
                COUNT(*) AS total_events,
                ROUND(AVG(text_length), 1) AS avg_text_length
            FROM reddit_events
            WHERE event_timestamp IS NOT NULL
            """,
            connection,
        )

        top_subreddit = pd.read_sql(
            """
            SELECT
                subreddit,
                COUNT(*) AS event_count
            FROM reddit_events
            WHERE event_timestamp IS NOT NULL
            GROUP BY subreddit
            ORDER BY event_count DESC
            LIMIT 1
            """,
            connection,
        )

        subreddit_activity = pd.read_sql(
            """
            SELECT
                subreddit,
                COUNT(*) AS event_count
            FROM reddit_events
            WHERE event_timestamp IS NOT NULL
            GROUP BY subreddit
            ORDER BY event_count DESC
            """,
            connection,
        )

        activity_over_time = pd.read_sql(
            """
            SELECT
                window_start,
                subreddit,
                event_count
            FROM event_window_summary
            ORDER BY window_start
            """,
            connection,
        )

        keyword_mentions = pd.read_sql(
            """
            SELECT
                COALESCE(SUM(mentions_python), 0) AS python,
                COALESCE(SUM(mentions_spark), 0) AS spark,
                COALESCE(SUM(mentions_kafka), 0) AS kafka
            FROM reddit_events
            WHERE event_timestamp IS NOT NULL
            """,
            connection,
        )

    total_events = int(metrics.loc[0, "total_events"])
    average_length = metrics.loc[0, "avg_text_length"]
    top_name = (
        top_subreddit.loc[0, "subreddit"]
        if not top_subreddit.empty
        else "No data yet"
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Events", f"{total_events:,}")

    with col2:
        st.metric("Top Subreddit", top_name)

    with col3:
        st.metric(
            "Average Text Length",
            average_length if pd.notna(average_length) else "No data yet",
        )

    st.subheader("Activity by Subreddit")

    if subreddit_activity.empty:
        st.info("Waiting for the first events.")
    else:
        st.bar_chart(
            subreddit_activity,
            x="subreddit",
            y="event_count",
        )

    st.subheader("Activity Over Time")

    if activity_over_time.empty:
        st.info("Waiting for the first completed streaming window.")
    else:
        activity_pivot = activity_over_time.pivot(
            index="window_start",
            columns="subreddit",
            values="event_count",
        )
        st.line_chart(activity_pivot)

    keyword_chart = keyword_mentions.T.reset_index()
    keyword_chart.columns = ["keyword", "mentions"]

    st.subheader("Technology Mentions")
    st.bar_chart(
        keyword_chart,
        x="keyword",
        y="mentions",
    )

    st.subheader("Recent Events")

    if recent_events.empty:
        st.info("Waiting for the first events.")
    else:
        st.dataframe(
            recent_events,
            use_container_width=True,
            hide_index=True,
        )


render_dashboard()
