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


st.subheader("Recent Events")
st.dataframe(recent_events, use_container_width=True)