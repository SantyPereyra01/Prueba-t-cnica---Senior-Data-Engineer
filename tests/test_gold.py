from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession

from saas_pipeline.gold import aggregate_daily_metrics


def test_daily_metrics_use_normalized_units_and_transaction_price(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            ("sv", date(2025, 3, 15), "ZPRE", Decimal("40"), Decimal("2"), 1, 10),
            ("sv", date(2025, 3, 15), "ZPRE", Decimal("5"), Decimal("3"), 2, 11),
            ("sv", date(2025, 3, 15), "ZPRE", Decimal("1"), Decimal("4"), 2, 11),
        ],
        "tenant_id string, fecha_proceso date, tipo_entrega string, "
        "cantidad_normalizada_st decimal(20,6), precio decimal(20,6), "
        "ruta long, transporte long",
    )

    result = aggregate_daily_metrics(facts).first()

    assert result.total_units == Decimal("46.000000")
    assert result.total_revenue == Decimal("99.000000")
    assert result.active_routes == 2
    assert result.active_transports == 2
