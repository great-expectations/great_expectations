import pytest

from great_expectations.validator.metric_configuration import MetricConfiguration


@pytest.fixture
def table_head_metric_config() -> MetricConfiguration:
    return MetricConfiguration(
        metric_name="table.head",
        metric_domain_kwargs={
            "batch_id": "abc123",
        },
        metric_value_kwargs={
            "n_rows": 5,
        },
    )


@pytest.fixture
def column_histogram_metric_config() -> MetricConfiguration:
    return MetricConfiguration(
        metric_name="column.histogram",
        metric_domain_kwargs={
            "batch_id": "def456",
        },
        metric_value_kwargs={
            "bins": 5,
        },
    )
