"""
Helper functions for the streamlit app.
"""

from typing import List

from dotenv import load_dotenv

from src.clients.s3_client import S3Client

# Load environment variables
load_dotenv()

# S3 Client
storage_client = S3Client.from_env()


# Function to load available seasons
def get_seasons() -> List[str]:
    """
    Checks and returns the list of seasons available.

    Returns:
        List[str]: e.g. [2023, 2024]
    """

    seasons = storage_client.list_directories(bucket="raw", prefix="")

    return seasons


# Function to load available Grands Prix from a selected year.
def get_grand_prix_names(year: int) -> List[str]:
    """
    Checks and returns the list of grands prix available for the selected year.

    Args:
        year: e.g. 2023, 2024.

    Returns:
        List[str]: e.g. ["Italian Grand Prix", "Monaco Grand Prix"]
    """

    grand_prix_names = storage_client.list_directories(bucket="raw", prefix=str(year))

    return grand_prix_names


def get_session_names(year: int, grand_prix_name: str) -> List[str]:
    """
    Checks and returns the list of session names available
    for the selected year and grand prix.

    Args:
        year: e.g. 2023, 2024.
        grand_prix_name: e.g. Italian Grand Prix

    Returns:
        List[str]: e.g. ["Race", "Qualifying"]
    """

    session_names = storage_client.list_directories(
        bucket="raw", prefix=f"{str(year)}/{grand_prix_name}"
    )

    return session_names
