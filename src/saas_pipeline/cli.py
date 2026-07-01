"""Command-line entrypoint for local and Databricks-compatible runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4

from saas_pipeline.config import ConfigurationError, load_config
from saas_pipeline.pipeline import run_tenant
from saas_pipeline.spark import build_spark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the SAAS multi-tenant data pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Execute Bronze, Silver and Gold")
    run.add_argument("--env", choices=("dev", "qa", "main"), default="dev")
    run.add_argument("--tenant", default="all", help="Tenant code or 'all'")
    run.add_argument("--start-date", help="Inclusive YYYY-MM-DD business date")
    run.add_argument("--end-date", help="Inclusive YYYY-MM-DD business date")
    run.add_argument("--fail-fast", action=argparse.BooleanOptionalAction, default=None)
    run.add_argument(
        "--project-root", type=Path, default=Path.cwd(), help="Repository root"
    )
    return parser


def _overrides(args: argparse.Namespace) -> dict[str, object]:
    execution: dict[str, object] = {}
    if args.start_date:
        execution["start_date"] = args.start_date
    if args.end_date:
        execution["end_date"] = args.end_date
    if args.fail_fast is not None:
        execution["fail_fast"] = args.fail_fast
    return {"execution": execution} if execution else {}


def execute(args: argparse.Namespace) -> int:
    overrides = _overrides(args)
    base = load_config(args.project_root, args.env, args.tenant, overrides)
    tenants = (
        list(base.tenants.enabled)
        if args.tenant.lower() == "all"
        else [args.tenant.lower()]
    )
    run_id = str(uuid4())
    spark = build_spark(base)
    summaries: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    try:
        for tenant in tenants:
            try:
                config = load_config(args.project_root, args.env, tenant, overrides)
                summaries.append(run_tenant(spark, config, tenant, run_id).to_dict())
            except Exception as exc:  # noqa: BLE001 - tenant isolation is intentional
                failures.append({"tenant": tenant, "error": str(exc)})
                if base.execution.fail_fast:
                    break
    finally:
        spark.stop()

    print(json.dumps({"run_id": run_id, "results": summaries, "failures": failures}, indent=2))
    return 1 if failures else 0


def main() -> None:
    try:
        args = build_parser().parse_args()
        raise SystemExit(execute(args))
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
