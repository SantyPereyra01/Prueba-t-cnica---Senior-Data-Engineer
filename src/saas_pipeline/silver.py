"""Silver facts, material SCD Type 2 and anomaly handling."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.paths import quarantine_path, table_path
from saas_pipeline.schemas import MATERIALS_RAW_SCHEMA
from saas_pipeline.storage import is_delta_table, merge_quarantine, merge_table
from saas_pipeline.transformations import (
    BUSINESS_KEY,
    find_scd2_overlaps,
    split_silver_records,
    try_cast,
)


class SCD2IntegrityError(ValueError):
    """Raised before fact processing when dimension intervals overlap."""


@dataclass(frozen=True)
class SilverResult:
    valid_records: int
    quarantined_records: int
    discarded_records: int
    conflicting_key_records: int
    dimensions: DataFrame


def load_material_dimension(
    spark: SparkSession,
    config: DictConfig,
    tenant: str,
    batch_id: str,
) -> DataFrame:
    """Upsert all supplied SCD2 versions using material + valid_from as key."""
    raw = (
        spark.read.option("header", True)
        .schema(MATERIALS_RAW_SCHEMA)
        .csv(config.input.materials)
    )
    dimensions = (
        raw.withColumn("precio_base", try_cast("precio_base", "decimal(38,18)"))
        .withColumn("valid_from", try_cast("valid_from", "date"))
        .withColumn("valid_to", try_cast("valid_to", "date"))
        .withColumn("is_current", F.lower(F.col("is_current")) == "true")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .dropDuplicates(["material", "valid_from"])
    )
    path = table_path(config, "silver", tenant, "dim_materials")
    merge_table(
        spark,
        dimensions,
        path,
        "target.material = source.material AND target.valid_from = source.valid_from",
    )
    return spark.read.format("delta").load(path)


def build_fact_deliveries(
    spark: SparkSession,
    config: DictConfig,
    tenant: str,
    dimensions: DataFrame,
) -> SilverResult:
    """Build and upsert enriched facts, persisting anomalies separately."""
    overlap_count = find_scd2_overlaps(dimensions).count()
    if overlap_count:
        raise SCD2IntegrityError(
            f"dim_materials contains {overlap_count} overlapping SCD2 interval(s)"
        )

    bronze_path = table_path(config, "bronze", tenant, "deliveries")
    if not is_delta_table(spark, bronze_path):
        raise FileNotFoundError(f"Bronze table does not exist: {bronze_path}")

    start_raw = config.execution.start_date.replace("-", "")
    end_raw = config.execution.end_date.replace("-", "")
    bronze = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("fecha_proceso").between(start_raw, end_raw))
    )
    valid, quarantined, discarded, conflicting = split_silver_records(bronze, dimensions)
    valid_count = valid.count()
    quarantine_count = quarantined.count()
    discarded_count = discarded.count()
    conflicting_count = conflicting.count()

    if quarantine_count:
        merge_quarantine(
            spark,
            quarantined,
            quarantine_path(config, "silver", tenant, "fact_deliveries"),
        )

    fact_path = table_path(config, "silver", tenant, "fact_deliveries")
    condition = " AND ".join(f"target.{key} = source.{key}" for key in BUSINESS_KEY)
    merge_table(spark, valid, fact_path, condition, partition_columns=["fecha_proceso"])
    return SilverResult(
        valid_count,
        quarantine_count,
        discarded_count,
        conflicting_count,
        dimensions,
    )
