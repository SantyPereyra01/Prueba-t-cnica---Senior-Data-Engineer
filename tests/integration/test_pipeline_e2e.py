from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]


def _prepare_project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    shutil.copytree(PROJECT_ROOT / "config", root / "config")
    shutil.copytree(PROJECT_ROOT / "tests" / "fixtures", root / "input")
    (root / "input" / "deliveries.csv").replace(
        root / "input" / "global_mobility_data_entrega_productos.csv"
    )
    if os.name == "nt":
        shutil.copytree(PROJECT_ROOT / ".spark-jars", root / ".spark-jars")
        shutil.copytree(PROJECT_ROOT / ".hadoop", root / ".hadoop")
    return root


def _run_pipeline(project_root: Path) -> dict:
    command = [
        sys.executable,
        "-m",
        "saas_pipeline.cli",
        "run",
        "--project-root",
        str(project_root),
        "--env",
        "dev",
        "--tenant",
        "sv",
        "--start-date",
        "2025-01-01",
        "--end-date",
        "2025-06-30",
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )
    payload, _ = json.JSONDecoder().raw_decode(completed.stdout.lstrip())
    return payload


def test_pipeline_is_idempotent_end_to_end(tmp_path: Path) -> None:
    project_root = _prepare_project(tmp_path)

    first = _run_pipeline(project_root)
    second = _run_pipeline(project_root)

    expected = {
        "tenant": "sv",
        "bronze_records": 6,
        "bronze_quarantined": 1,
        "silver_records": 2,
        "silver_quarantined": 2,
        "silver_discarded": 1,
        "gold_records": 2,
    }
    for run in (first, second):
        assert run["failures"] == []
        result = run["results"][0]
        assert {key: result[key] for key in expected} == expected
