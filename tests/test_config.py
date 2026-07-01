from pathlib import Path

import pytest

from saas_pipeline.config import ConfigurationError, load_config

PROJECT_ROOT = Path(__file__).parents[1]


def test_hierarchical_config_applies_environment_and_tenant() -> None:
    config = load_config(PROJECT_ROOT, environment="qa", tenant="sv")

    assert config.environment == "qa"
    assert config.tenant.code == "sv"
    assert config.execution.tenant == "sv"
    assert config.quality.fail_on_critical is True
    assert config.paths.bronze.endswith("data/qa/bronze") or config.paths.bronze.endswith(
        r"data\qa\bronze"
    )


def test_config_rejects_inverted_date_range() -> None:
    with pytest.raises(ConfigurationError, match="cannot be after"):
        load_config(
            PROJECT_ROOT,
            environment="dev",
            tenant="sv",
            overrides={
                "execution": {"start_date": "2025-06-30", "end_date": "2025-01-01"}
            },
        )
