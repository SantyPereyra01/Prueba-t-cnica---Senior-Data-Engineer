"""Bronze ingestion with tenant isolation and partition-level idempotency."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.paths import quarantine_path, table_path
from saas_pipeline.schemas import DELIVERIES_RAW_SCHEMA
from saas_pipeline.storage import merge_quarantine, overwrite_date_range
from saas_pipeline.transformations import parse_process_date, source_record_hash


@dataclass(frozen=True)
class BronzeResult:
    records_written: int
    records_quarantined: int


def ingest_bronze(
    spark: SparkSession,
    config: DictConfig,
    tenant: str,
    batch_id: str,
) -> BronzeResult:
    """Read raw deliveries and replace the requested tenant/date partitions."""
    source = (
        spark.read.option("header", True)
        .schema(DELIVERIES_RAW_SCHEMA)
        .csv(config.input.deliveries)
    )
    tenant_rows = (
        source.filter(F.lower(F.col("pais")) == tenant)
        .withColumn("pais", F.lower(F.col("pais")))
        .withColumn("_tenant_id", F.lit(tenant))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_record_hash", source_record_hash())
        .withColumn("_parsed_process_date", parse_process_date())
    )

    invalid_dates = (
        tenant_rows.filter(F.col("_parsed_process_date").isNull())
        .drop("_parsed_process_date")
        .withColumn("_quarantine_reason", F.lit("invalid_or_null_fecha_proceso"))
    )
    invalid_count = invalid_dates.count()
    if invalid_count:
        merge_quarantine(
            spark,
            invalid_dates,
            quarantine_path(config, "bronze", tenant, "deliveries"),
        )

    start_raw = config.execution.start_date.replace("-", "")
    end_raw = config.execution.end_date.replace("-", "")
    valid = (
        tenant_rows.filter(F.col("_parsed_process_date").isNotNull())
        .filter(F.col("fecha_proceso").between(start_raw, end_raw))
        .drop("_parsed_process_date")
    )
    written = valid.count()
    overwrite_date_range(
        spark,
        valid,
        table_path(config, "bronze", tenant, "deliveries"),
        "fecha_proceso",
        start_raw,
        end_raw,
    )
    return BronzeResult(written, invalid_count)
