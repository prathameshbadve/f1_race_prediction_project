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

st.write("- Results visualization")
st.write("- Lap by lap position changes")
st.write("- Tyre strategies used")

st.write("---")

st.title("Data Validation Summary")

c4, c5 = st.columns(
    spec=[1, 1],
    gap="small",
    vertical_alignment="center",
    border=True,
)


def format_data_type(chosen_type):
    """Format data type and return value to display"""

    formatted_values = {
        "season_schedule": "Season Schedule",
        "results": "Results",
        "laps": "Laps",
        "weather": "Weather",
        "messages": "Race Control Messages",
        "track_status": "Track Status",
        "session_status": "Session Status",
        "session_info": "Session Info",
    }

    return formatted_values[chosen_type]


with c4:
    data_type_selection = st.selectbox(
        label="Choose data type",
        options=[
            "season_schedule",
            "results",
            "laps",
            "weather",
            "messages",
            "track_status",
            "session_status",
            "session_info",
        ],
        format_func=format_data_type,
    )

validation_results_keys = storage_client.list_files(
    bucket="processed",
    prefix=f"silver/validation_results/{data_type_selection}/",
)
validation_results_keys.sort(reverse=True)
with c5:
    file_selection = st.selectbox(
        label="Choose file version",
        options=validation_results_keys,
    )

if file_selection is not None:
    file_key = f"silver/validation_results/{data_type_selection}/{file_selection}"

    validation_results = storage_client.download_dataframe(
        bucket="processed",
        key=file_key,
    )

    st.dataframe(validation_results)
