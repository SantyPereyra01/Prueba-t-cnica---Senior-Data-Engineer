"""Per-tenant orchestration for the medallion pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from uuid import uuid4

from omegaconf import DictConfig
from pyspark.sql import SparkSession

from saas_pipeline.bronze import ingest_bronze
from saas_pipeline.gold import build_daily_metrics
from saas_pipeline.quality import (
    has_critical_failure,
    persist_quality_results,
    run_silver_checks,
)
from saas_pipeline.silver import build_fact_deliveries, load_material_dimension


class QualityGateError(RuntimeError):
    """Raised when a configured critical quality gate blocks Gold."""


@dataclass(frozen=True)
class TenantRunSummary:
    tenant: str
    batch_id: str
    bronze_records: int
    bronze_quarantined: int
    silver_records: int
    silver_quarantined: int
    silver_discarded: int
    gold_records: int

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


def run_tenant(
    spark: SparkSession, config: DictConfig, tenant: str, run_id: str
) -> TenantRunSummary:
    """Execute Bronze -> Silver -> quality gate -> Gold for one tenant."""
    batch_id = str(uuid4())
    bronze = ingest_bronze(spark, config, tenant, batch_id)
    dimensions = load_material_dimension(spark, config, tenant, batch_id)
    silver = build_fact_deliveries(spark, config, tenant, dimensions)
    checks = run_silver_checks(
        spark,
        config,
        tenant,
        silver.quarantined_records,
        silver.discarded_records,
    )
    persist_quality_results(spark, config, run_id, batch_id, tenant, checks)
    if config.quality.fail_on_critical and has_critical_failure(checks):
        failed = [
            item.check_name
            for item in checks
            if item.severity == "critical" and not item.passed
        ]
        raise QualityGateError(f"Critical quality checks failed for {tenant}: {failed}")

    gold = build_daily_metrics(spark, config, tenant)
    return TenantRunSummary(
        tenant=tenant,
        batch_id=batch_id,
        bronze_records=bronze.records_written,
        bronze_quarantined=bronze.records_quarantined,
        silver_records=silver.valid_records,
        silver_quarantined=silver.quarantined_records,
        silver_discarded=silver.discarded_records,
        gold_records=gold.records_written,
    )
