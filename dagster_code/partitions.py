"""
Partitions for the F1 seasons
"""

from typing import List

from dagster import StaticPartitionsDefinition


def get_f1_season_partitions() -> List[str]:
    """
    Get all season partitions from 2015 - 2025
    """

    return [str(year) for year in range(2015, 2026)]


try:
    partition_keys = get_f1_season_partitions()
    f1_season_partitions = StaticPartitionsDefinition(partition_keys=partition_keys)

except Exception:  # pylint: disable=broad-except
    f1_season_partitions = StaticPartitionsDefinition(partition_keys=[])
