"""Pure DataFrame transformations shared by the pipeline and unit tests."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from saas_pipeline.schemas import DELIVERY_SOURCE_COLUMNS

VALID_DELIVERY_TYPES = ("ZPRE", "ZVE1", "Z04", "Z05")
ROUTINE_DELIVERY_TYPES = ("ZPRE", "ZVE1")
BONUS_DELIVERY_TYPES = ("Z04", "Z05")
BUSINESS_KEY = [
    "tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]
MONEY_TYPE = DecimalType(38, 18)


def try_cast(column: str, data_type: str) -> F.Column:
    """Cast malformed values to null consistently, including under ANSI mode."""
    return F.expr(f"try_cast(`{column}` as {data_type})")


def source_record_hash() -> F.Column:
    """Stable hash over source fields; nulls remain distinguishable from empty strings."""
    values = [F.coalesce(F.col(name), F.lit("<NULL>")) for name in DELIVERY_SOURCE_COLUMNS]
    return F.sha2(F.concat_ws("||", *values), 256)


def parse_process_date(column: str = "fecha_proceso") -> F.Column:
    raw = F.col(column)
    parsed = F.expr(f"try_to_timestamp(`{column}`, 'yyyyMMdd')").cast("date")
    return F.when(raw.rlike(r"^[0-9]{8}$"), parsed)


def normalize_deliveries(frame: DataFrame) -> DataFrame:
    """Type raw delivery fields and normalize all quantities to stock units (ST)."""
    typed = (
        frame.withColumn("fecha_proceso", parse_process_date())
        .withColumn("transporte", try_cast("transporte", "bigint"))
        .withColumn("ruta", try_cast("ruta", "bigint"))
        .withColumn("precio", try_cast("precio", "decimal(38,18)"))
        .withColumn("cantidad", try_cast("cantidad", "decimal(38,18)"))
        .withColumn("unidad", F.upper(F.trim(F.col("unidad"))))
        .withColumn("tipo_entrega", F.upper(F.trim(F.col("tipo_entrega"))))
        .withColumn("tenant_id", F.lower(F.col("_tenant_id")))
    )
    return (
        typed.withColumn(
            "cantidad_normalizada_st",
            F.when(F.col("unidad") == "CS", F.col("cantidad") * F.lit(20))
            .when(F.col("unidad") == "ST", F.col("cantidad"))
            .cast(MONEY_TYPE),
        )
        .withColumn("is_routine_delivery", F.col("tipo_entrega").isin(*ROUTINE_DELIVERY_TYPES))
        .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(*BONUS_DELIVERY_TYPES))
    )


def attach_material_dimension(facts: DataFrame, dimensions: DataFrame) -> DataFrame:
    """Enrich facts using the material version effective on the business date."""
    fact = facts.alias("fact")
    dim = dimensions.alias("dim")
    condition = (F.col("fact.material") == F.col("dim.material")) & F.col(
        "fact.fecha_proceso"
    ).between(F.col("dim.valid_from"), F.col("dim.valid_to"))
    return fact.join(dim, condition, "left").select(
        "fact.*",
        F.col("dim.descripcion").alias("material_descripcion"),
        F.col("dim.categoria").alias("material_categoria"),
        F.col("dim.precio_base").alias("material_precio_base"),
        F.col("dim.valid_from").alias("material_valid_from"),
        F.col("dim.valid_to").alias("material_valid_to"),
    )


def find_scd2_overlaps(dimensions: DataFrame) -> DataFrame:
    """Return material versions whose start overlaps the previous inclusive interval."""
    window = Window.partitionBy("material").orderBy("valid_from", "valid_to")
    return (
        dimensions.withColumn("_previous_valid_to", F.lag("valid_to").over(window))
        .filter(
            F.col("_previous_valid_to").isNotNull()
            & (F.col("valid_from") <= F.col("_previous_valid_to"))
        )
        .drop("_previous_valid_to")
    )


def find_business_key_conflicts(frame: DataFrame) -> DataFrame:
    """Return business keys represented by more than one distinct source payload."""
    return frame.groupBy(*BUSINESS_KEY).count().filter(F.col("count") > 1).drop("count")


def add_quarantine_reason(frame: DataFrame) -> DataFrame:
    """Classify all Silver anomalies into a single auditable reason field."""
    return frame.withColumn(
        "_quarantine_reason",
        F.concat_ws(
            ";",
            F.when(
                F.col("cantidad_normalizada_st").isNull()
                | (F.col("cantidad_normalizada_st") <= 0),
                F.lit("invalid_or_non_positive_quantity"),
            ),
            F.when(F.col("precio").isNull(), F.lit("invalid_or_null_price")),
            F.when(~F.col("unidad").isin("CS", "ST"), F.lit("unsupported_unit")),
            F.when(F.col("material_valid_from").isNull(), F.lit("material_not_effective")),
        ),
    )


def split_silver_records(
    bronze: DataFrame, dimensions: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Return valid, quarantined, discarded and conflicting records."""
    deduplicated = bronze.dropDuplicates(DELIVERY_SOURCE_COLUMNS)
    normalized = normalize_deliveries(deduplicated)
    discarded = normalized.filter(~F.col("tipo_entrega").isin(*VALID_DELIVERY_TYPES)).withColumn(
        "_discard_reason", F.lit("unsupported_delivery_type")
    )
    candidates = normalized.filter(F.col("tipo_entrega").isin(*VALID_DELIVERY_TYPES))
    conflicting_keys = find_business_key_conflicts(candidates)
    conflicting_source = candidates.join(conflicting_keys, BUSINESS_KEY, "inner")
    unique_source = candidates.join(conflicting_keys, BUSINESS_KEY, "left_anti")

    classified = add_quarantine_reason(attach_material_dimension(unique_source, dimensions))
    conflicting = add_quarantine_reason(
        attach_material_dimension(conflicting_source, dimensions)
    ).withColumn(
        "_quarantine_reason",
        F.concat_ws(
            ";",
            F.lit("conflicting_business_key"),
            F.when(F.col("_quarantine_reason") != "", F.col("_quarantine_reason")),
        ),
    )
    quarantined = classified.filter(F.col("_quarantine_reason") != "").unionByName(
        conflicting
    )
    valid = classified.filter(F.col("_quarantine_reason") == "").drop("_quarantine_reason")
    return valid, quarantined, discarded, conflicting
