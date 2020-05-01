import datetime

import pytest

from great_expectations.core import RunIdentifier
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.data_context.util import instantiate_class_from_config


@pytest.fixture(params=[
    {
        "class_name": "MetricStore",
        "store_backend": {
            "class_name": "DatabaseStoreBackend",
            "credentials": {
                "drivername": "postgresql",
                "username": "postgres",
                "password": "",
                "host": "localhost",
                "port": "5432",
                "database": "test_ci"
            }
        }
    },
    {
        "class_name": "MetricStore",
        "module_name": "great_expectations.data_context.store"
    }
])
def param_store(request, test_backends):
    # If we have a backend configuration but we do not have postgres configured, skip
    backend_config = request.param.get("store_backend", None)
    if backend_config:
        if backend_config.get("credentials", {}).get("drivername", None) == "postgresql":
            if "postgresql" not in test_backends:
                pytest.skip("skipping fixture because postgresql not selected")

    return instantiate_class_from_config(
        config=request.param,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
        },
        runtime_environment={}
    )


def test_metric_store_remove_key(param_store):
    run_id = RunIdentifier(run_name=datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ"))
    metric_identifier = ValidationMetricIdentifier(
        run_id=run_id,
        data_asset_name=None,
        expectation_suite_identifier="asset.warning",
        metric_name="expect_column_values_to_match_regex.result.unexpected_percent",
        metric_kwargs_id="column=mycol"
    )
    metric_value = 12.3456789
    assert not param_store.has_key(metric_identifier)
    param_store.set(metric_identifier, metric_value)
    assert param_store.has_key(metric_identifier)
    assert param_store.get(metric_identifier) == metric_value
    param_store.remove_key(metric_identifier)
    assert not param_store.has_key(metric_identifier)
