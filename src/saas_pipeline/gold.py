"""Gold business aggregates."""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from saas_pipeline.paths import table_path
from saas_pipeline.storage import overwrite_date_range


@dataclass(frozen=True)
class GoldResult:
    records_written: int


def aggregate_daily_metrics(facts: DataFrame) -> DataFrame:
    """Aggregate valid Silver facts at the required Gold grain."""
    return (
        facts.groupBy("tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            F.sum("cantidad_normalizada_st").alias("total_units"),
            F.sum(F.col("cantidad_normalizada_st") * F.col("precio"))
            .cast(DecimalType(38, 6))
            .alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
        .withColumn("_aggregation_timestamp", F.current_timestamp())
    )


def build_daily_metrics(
    spark: SparkSession, config: DictConfig, tenant: str
) -> GoldResult:
    """Recompute requested Gold partitions from the authoritative Silver fact."""
    facts = (
        spark.read.format("delta")
        .load(table_path(config, "silver", tenant, "fact_deliveries"))
        .filter(
            F.col("fecha_proceso").between(
                config.execution.start_date, config.execution.end_date
            )
        )
    )
    metrics = aggregate_daily_metrics(facts)
    count = metrics.count()
    overwrite_date_range(
        spark,
        metrics,
        table_path(config, "gold", tenant, "daily_metrics_by_delivery_type"),
        "fecha_proceso",
        config.execution.start_date,
        config.execution.end_date,
    )
    return GoldResult(count)
