"""Logical table-to-storage path mapping."""

from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig


def table_path(config: DictConfig, layer: str, tenant: str, table: str) -> str:
    return str(Path(config.paths[layer]) / tenant / table)


def quarantine_path(config: DictConfig, layer: str, tenant: str, table: str) -> str:
    return str(Path(config.paths.quarantine_root) / f"{layer}_quarantine" / tenant / table)
