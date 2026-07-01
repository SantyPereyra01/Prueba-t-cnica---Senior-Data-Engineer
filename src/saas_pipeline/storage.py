"""Delta Lake write primitives used by every layer."""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def is_delta_table(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def overwrite_date_range(
    spark: SparkSession,
    frame: DataFrame,
    path: str,
    partition_column: str,
    start_value: str,
    end_value: str,
) -> None:
    """Atomically replace a date range, including removal of stale rows."""
    if not is_delta_table(spark, path):
        frame.write.format("delta").mode("overwrite").partitionBy(partition_column).save(path)
        return

    predicate = (
        f"{partition_column} >= '{start_value}' AND {partition_column} <= '{end_value}'"
    )
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .save(path)
    )


def merge_table(
    spark: SparkSession,
    frame: DataFrame,
    path: str,
    condition: str,
    partition_columns: Sequence[str] = (),
) -> None:
    """Upsert a frame into a Delta table."""
    if not is_delta_table(spark, path):
        writer = frame.write.format("delta").mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.save(path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_quarantine(spark: SparkSession, frame: DataFrame, path: str) -> None:
    """Persist quarantine records idempotently by source-content hash and reason."""
    merge_table(
        spark,
        frame,
        path,
        "target._record_hash = source._record_hash "
        "AND target._quarantine_reason = source._quarantine_reason",
    )
