"""Hierarchical configuration loading and validation."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from omegaconf import DictConfig, OmegaConf


class ConfigurationError(ValueError):
    """Raised when the effective configuration is invalid."""


def _parse_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{field} must use YYYY-MM-DD: {value!r}") from exc


def _make_paths_absolute(config: DictConfig, project_root: Path) -> None:
    for key in ("deliveries", "materials"):
        value = Path(config.input[key])
        config.input[key] = str(value if value.is_absolute() else project_root / value)
    for key in ("bronze", "silver", "gold", "quarantine_root", "quality_logs"):
        value = Path(config.paths[key])
        config.paths[key] = str(value if value.is_absolute() else project_root / value)


def validate_config(config: DictConfig) -> None:
    """Validate cross-field constraints after all overrides are applied."""
    start = _parse_date(config.execution.start_date, "execution.start_date")
    end = _parse_date(config.execution.end_date, "execution.end_date")
    if start > end:
        raise ConfigurationError("execution.start_date cannot be after execution.end_date")

    enabled = {str(code).lower() for code in config.tenants.enabled}
    selected = str(config.execution.tenant).lower()
    if selected != "all" and selected not in enabled:
        raise ConfigurationError(
            f"Unknown tenant {selected!r}; expected one of {sorted(enabled)} or 'all'"
        )

    missing_inputs = [
        str(path)
        for path in (Path(config.input.deliveries), Path(config.input.materials))
        if not path.is_file()
    ]
    if missing_inputs:
        raise ConfigurationError(f"Input files not found: {', '.join(missing_inputs)}")


def load_config(
    project_root: str | Path,
    environment: str = "dev",
    tenant: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> DictConfig:
    """Load base, environment and optional tenant YAML files in precedence order."""
    root = Path(project_root).resolve()
    config_dir = root / "config"
    env_path = config_dir / "env" / f"{environment}.yaml"
    if not env_path.is_file():
        raise ConfigurationError(f"Unknown environment {environment!r}: {env_path}")

    layers: list[DictConfig] = [OmegaConf.load(config_dir / "base.yaml"), OmegaConf.load(env_path)]
    selected = tenant.lower() if tenant else None
    if selected and selected != "all":
        tenant_path = config_dir / "tenants" / f"{selected}.yaml"
        if not tenant_path.is_file():
            raise ConfigurationError(f"Tenant configuration not found: {tenant_path}")
        layers.append(OmegaConf.load(tenant_path))

    config = OmegaConf.merge(*layers)
    if selected:
        config.execution.tenant = selected
    if overrides:
        config = OmegaConf.merge(config, OmegaConf.create(overrides))

    _make_paths_absolute(config, root)
    config.project_root = str(root)
    OmegaConf.resolve(config)
    validate_config(config)
    return config
