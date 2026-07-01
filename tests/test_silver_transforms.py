from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession

from saas_pipeline.transformations import (
    attach_material_dimension,
    normalize_deliveries,
    split_silver_records,
)


def _delivery(
    *,
    material: str = "SKU-1",
    quantity: str = "2",
    unit: str = "CS",
    delivery_type: str = "ZPRE",
) -> dict[str, str]:
    return {
        "pais": "sv",
        "fecha_proceso": "20250415",
        "transporte": "10",
        "ruta": "20",
        "tipo_entrega": delivery_type,
        "material": material,
        "precio": "3.50",
        "cantidad": quantity,
        "unidad": unit,
        "_tenant_id": "sv",
        "_batch_id": "batch",
        "_record_hash": f"hash-{material}-{quantity}-{delivery_type}",
        "_source_file": "test.csv",
    }


def _dimensions(spark: SparkSession):
    return spark.createDataFrame(
        [
            (
                "SKU-1",
                "Old description",
                "DRINKS",
                Decimal("3.00"),
                date(2025, 1, 1),
                date(2025, 3, 31),
                False,
            ),
            (
                "SKU-1",
                "Current description",
                "DRINKS",
                Decimal("3.25"),
                date(2025, 4, 1),
                date(9999, 12, 31),
                True,
            ),
        ],
        "material string, descripcion string, categoria string, precio_base decimal(38,18), "
        "valid_from date, valid_to date, is_current boolean",
    )


def test_normalizes_cases_to_stock_units(spark: SparkSession) -> None:
    frame = spark.createDataFrame([_delivery(quantity="2", unit="CS")])

    result = normalize_deliveries(frame).first()

    assert result.cantidad_normalizada_st == Decimal("40.000000000000000000")
    assert result.is_routine_delivery is True
    assert result.is_bonus_delivery is False


def test_temporal_join_uses_effective_version_not_current_flag(spark: SparkSession) -> None:
    fact = normalize_deliveries(
        spark.createDataFrame([{**_delivery(), "fecha_proceso": "20250315"}])
    )

    result = attach_material_dimension(fact, _dimensions(spark)).first()

    assert result.material_descripcion == "Old description"
    assert result.material_precio_base == Decimal("3.000000000000000000")


def test_split_quarantines_invalid_quantity_and_unknown_material(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [
            _delivery(material="SKU-1", quantity="-1"),
            _delivery(material="UNKNOWN", quantity="2"),
        ]
    )

    valid, quarantine, discarded = split_silver_records(source, _dimensions(spark))
    reasons = {row._quarantine_reason for row in quarantine.collect()}

    assert valid.count() == 0
    assert discarded.count() == 0
    assert reasons == {"invalid_or_non_positive_quantity", "material_not_effective"}


def test_split_discards_out_of_scope_delivery_type(spark: SparkSession) -> None:
    source = spark.createDataFrame([_delivery(delivery_type="COBR")])

    valid, quarantine, discarded = split_silver_records(source, _dimensions(spark))

    assert valid.count() == 0
    assert quarantine.count() == 0
    assert discarded.first()._discard_reason == "unsupported_delivery_type"
