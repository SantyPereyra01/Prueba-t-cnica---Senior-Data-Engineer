"""Data-quality checks and shared Delta audit logging."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from saas_pipeline.paths import table_path
from saas_pipeline.transformations import (
    BUSINESS_KEY,
    VALID_DELIVERY_TYPES,
    find_scd2_overlaps,
)


@dataclass(frozen=True)
class QualityResult:
    check_name: str
    severity: str
    records_checked: int
    records_failed: int

    @property
    def passed(self) -> bool:
        return self.records_failed == 0


QUALITY_LOG_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)


def _predicate_check(
    frame: DataFrame, check_name: str, severity: str, failure_condition: F.Column
) -> QualityResult:
    counts = frame.agg(
        F.count(F.lit(1)).alias("checked"),
        F.sum(F.when(failure_condition, 1).otherwise(0)).alias("failed"),
    ).first()
    return QualityResult(check_name, severity, counts.checked or 0, counts.failed or 0)


def run_silver_checks(
    spark: SparkSession,
    config: DictConfig,
    tenant: str,
    quarantined_records: int,
    discarded_records: int,
    conflicting_key_records: int,
) -> list[QualityResult]:
    """Evaluate fact integrity and SCD2 invariants on persisted Silver tables."""
    fact = spark.read.format("delta").load(
        table_path(config, "silver", tenant, "fact_deliveries")
    )
    start = config.execution.start_date
    end = config.execution.end_date
    fact = fact.filter(F.col("fecha_proceso").between(start, end))
    dimensions = spark.read.format("delta").load(
        table_path(config, "silver", tenant, "dim_materials")
    )

    duplicate_keys = fact.groupBy(*BUSINESS_KEY).count().filter(F.col("count") > 1).count()
    current_counts = dimensions.filter("is_current").groupBy("material").count()
    invalid_current_materials = (
        dimensions.select("material")
        .distinct()
        .join(current_counts, "material", "left")
        .fillna(0, subset=["count"])
        .filter(F.col("count") != 1)
        .count()
    )
    invalid_intervals = dimensions.filter(
        F.col("valid_from").isNull()
        | F.col("valid_to").isNull()
        | (F.col("valid_from") > F.col("valid_to"))
    ).count()
    overlapping_intervals = find_scd2_overlaps(dimensions).count()
    fact_count = fact.count()

    results = [
        _predicate_check(
            fact,
            "positive_normalized_quantity",
            "critical",
            F.col("cantidad_normalizada_st").isNull()
            | (F.col("cantidad_normalizada_st") <= 0),
        ),
        _predicate_check(
            fact,
            "valid_delivery_type",
            "critical",
            ~F.col("tipo_entrega").isin(*VALID_DELIVERY_TYPES),
        ),
        _predicate_check(
            fact,
            "material_enrichment_complete",
            "critical",
            F.col("material_valid_from").isNull(),
        ),
        QualityResult(
            "business_key_uniqueness", "critical", fact_count, duplicate_keys
        ),
        QualityResult(
            "single_current_material_version",
            "critical",
            dimensions.select("material").distinct().count(),
            invalid_current_materials,
        ),
        QualityResult(
            "valid_material_intervals", "critical", dimensions.count(), invalid_intervals
        ),
        QualityResult(
            "scd2_intervals_do_not_overlap",
            "critical",
            dimensions.count(),
            overlapping_intervals,
        ),
        QualityResult(
            "business_key_conflicts",
            "critical",
            fact_count + conflicting_key_records,
            conflicting_key_records,
        ),
        QualityResult(
            "quarantined_records",
            "warning",
            fact_count + quarantined_records,
            quarantined_records,
        ),
        QualityResult(
            "discarded_delivery_types", "info", fact_count + discarded_records, discarded_records
        ),
    ]
    return results


def persist_quality_results(
    spark: SparkSession,
    config: DictConfig,
    run_id: str,
    batch_id: str,
    tenant: str,
    results: list[QualityResult],
) -> None:
    executed_at = datetime.now(UTC)
    dimension_checks = {
        "single_current_material_version",
        "valid_material_intervals",
        "scd2_intervals_do_not_overlap",
    }
    rows = [
        (
            run_id,
            batch_id,
            tenant,
            "silver",
            "dim_materials" if item.check_name in dimension_checks else "fact_deliveries",
            item.check_name,
            item.severity,
            item.records_checked,
            item.records_failed,
            item.passed,
            executed_at,
        )
        for item in results
    ]
    (
        spark.createDataFrame(rows, QUALITY_LOG_SCHEMA)
        .write.format("delta")
        .mode("append")
        .save(config.paths.quality_logs)
    )


def has_critical_failure(results: list[QualityResult]) -> bool:
    return any(item.severity == "critical" and not item.passed for item in results)
