"""Refactor del ejemplo del Anexo A usando transformaciones Spark declarativas."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class DeliveryJobConfig:
    input_path: str
    output_path: str
    tenant: str
    cases_to_stock_units: int = 20


def transform_deliveries(frame: DataFrame, config: DeliveryJobConfig) -> DataFrame:
    """Filtra y normaliza entregas sin sacar datos del motor distribuido."""
    normalized_quantity = (
        F.when(
            F.col("unidad") == "CS",
            F.col("cantidad") * F.lit(config.cases_to_stock_units),
        )
        .when(F.col("unidad") == "ST", F.col("cantidad"))
        .otherwise(F.lit(None))
    )
    return (
        frame.filter(F.lower(F.col("pais")) == config.tenant.lower())
        .filter(F.col("tipo_entrega").isin("ZPRE", "ZVE1"))
        .withColumn("cantidad_st", normalized_quantity)
        .filter(F.col("cantidad_st").isNotNull() & (F.col("cantidad_st") > 0))
        .withColumn("total", F.col("cantidad_st") * F.col("precio"))
        .select(
            F.lower("pais").alias("tenant_id"),
            F.to_date("fecha_proceso", "yyyyMMdd").alias("fecha_proceso"),
            "material",
            "cantidad_st",
            "total",
        )
    )


def process(spark: SparkSession, config: DeliveryJobConfig) -> DataFrame:
    source = spark.read.option("header", True).option("inferSchema", True).csv(config.input_path)
    result = transform_deliveries(source, config)
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"tenant_id = '{config.tenant.lower()}'")
        .save(config.output_path)
    )
    return result
