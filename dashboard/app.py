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


st.title("Streaming Analytics Dashboard")


with get_connection() as connection:

    # Get the 20 most recent events
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

    # Get total events and average text length
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

    # Find the subreddit with the most events
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


# --------------------------------------------------
# DISPLAY SUMMARY METRICS
# --------------------------------------------------

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Events",
        int(metrics.loc[0, "total_events"]),
    )

with col2:
    st.metric(
        "Top Subreddit",
        top_subreddit.loc[0, "subreddit"],
    )

with col3:
    st.metric(
        "Average Text Length",
        metrics.loc[0, "avg_text_length"],
    )


# --------------------------------------------------
# DISPLAY RECENT EVENTS
# --------------------------------------------------

st.subheader("Recent Events")

st.dataframe(
    recent_events,
    use_container_width=True,
)