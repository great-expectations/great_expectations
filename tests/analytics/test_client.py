import posthog
import pytest

from great_expectations.analytics.client import init as init_analytics
from great_expectations.analytics.config import ENV_CONFIG


@pytest.mark.unit
@pytest.mark.parametrize(
    "enabled_from_config, enabled_from_env_var, expected_posthog_disabled",
    [
        pytest.param(True, True, False, id="both_true"),
        pytest.param(True, False, True, id="config_true_env_false"),
        pytest.param(False, True, True, id="config_false_env_true"),
        pytest.param(False, False, True, id="both_false"),
    ],
)
def test_init_respects_both_config_and_env_var_inputs(
    monkeypatch,
    enabled_from_config: bool,
    enabled_from_env_var: bool,
    expected_posthog_disabled: bool,
):
    monkeypatch.setattr(ENV_CONFIG, "gx_analytics_enabled", enabled_from_env_var)

    init_analytics(enabled_from_config=enabled_from_config)
    assert expected_posthog_disabled == posthog.disabled
