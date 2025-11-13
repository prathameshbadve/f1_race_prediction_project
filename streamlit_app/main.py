"""
Streamlit app to view data and do inference on ML model
"""

import streamlit as st

from src.clients.s3_client import S3Client
from streamlit_app.utils.funcs import (
    get_seasons,
    get_grand_prix_names,
    get_session_names,
)

# S3 Client
storage_client = S3Client.from_env()


## MAIN APP

st.set_page_config(layout="wide", page_title="F1 Race Prediction")

st.title("F1 Race Prediction")

c1, c2, c3 = st.columns(
    spec=[1, 1, 1], gap="small", vertical_alignment="center", border=True
)

seasons = get_seasons()
with c1:
    year_selection = st.selectbox(
        label="Year",
        options=seasons,
    )

grand_prix_names = get_grand_prix_names(year=year_selection)
with c2:
    grand_prix_selection = st.selectbox(
        label="Grand Prix",
        options=grand_prix_names,
    )

session_names = get_session_names(
    year=year_selection, grand_prix_name=grand_prix_selection
)
with c3:
    session_selection = st.selectbox(
        label="Session",
        options=session_names,
    )

object_key = (
    f"{year_selection}/{grand_prix_selection}/{session_selection}/results.parquet"
)
results_df = storage_client.download_dataframe(bucket="raw", key=object_key)
results_df = results_df[
    [
        "FullName",
        "DriverNumber",
        "Abbreviation",
        "TeamName",
        "TeamColor",
        "ClassifiedPosition",
        "Status",
        "Points",
    ]
]

st.dataframe(results_df, hide_index=True)

st.write("Lap by lap position changes.")

st.write("---")

st.title("Data Validation Summary")

data_type_selection = st.selectbox(
    label="Choose data type",
    options=[
        "Season Schedule",
        "Results",
        "Laps",
        "Weather",
        "Race Control Messages",
        "Track Status",
        "Session Status",
        "Session Info",
    ],
)

schedule_validation_result_keys = storage_client.list_files(
    bucket="processed",
    prefix="silver/validation_results/season_schedule/",
)

st.write(schedule_validation_result_keys)
