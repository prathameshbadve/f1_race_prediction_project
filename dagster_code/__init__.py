"""
F1 Data Pipeline - Dagster Definitions

This file MUST export a 'defs' object for Dagster to find it.
"""

from dagster import Definitions, asset


@asset
def hello_f1():
    """
    Minimal test asset to verify Dagster is working.
    Replace this with your actual F1 data pipeline logic.
    """
    return "Hello from F1 Data Pipeline!"


@asset
def hello_world():
    """
    Another minimal test
    """
    return "Hello World!"


# CRITICAL: This 'defs' variable must be present and named exactly 'defs'
defs = Definitions(
    assets=[
        hello_f1,
        hello_world,
    ],
)
