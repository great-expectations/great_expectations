import pytest

from great_expectations.validator.metric_configuration import MetricConfiguration


@pytest.mark.unit
def test_metric_configuration__repr__and__str__(
    table_head_metric_config: MetricConfiguration,
) -> None:
    expected = '{\n  "metric_name": "table.head",\n  "metric_domain_kwargs": {\n    "batch_id": "abc123"\n  },\n  "metric_domain_kwargs_id": "batch_id=abc123",\n  "metric_value_kwargs": {\n    "n_rows": 5\n  },\n  "metric_value_kwargs_id": "n_rows=5",\n  "id": [\n    "table.head",\n    "batch_id=abc123",\n    "n_rows=5"\n  ]\n}'
    assert table_head_metric_config.__repr__() == expected
    assert table_head_metric_config.__str__() == expected


@pytest.mark.unit
def test_metric_configuration_metric_dependencies_property_and_setter(
    table_head_metric_config: MetricConfiguration,
) -> None:
    assert table_head_metric_config.metric_dependencies == {}

    table_head_metric_config.metric_dependencies = {"e": "f"}

    assert table_head_metric_config.metric_dependencies == {"e": "f"}


@pytest.mark.unit
def test_metric_configuration_to_json_dict(
    table_head_metric_config: MetricConfiguration,
) -> None:
    assert table_head_metric_config.to_json_dict() == {
        "id": ["table.head", "batch_id=abc123", "n_rows=5"],
        "metric_domain_kwargs": {"batch_id": "abc123"},
        "metric_domain_kwargs_id": "batch_id=abc123",
        "metric_name": "table.head",
        "metric_value_kwargs": {"n_rows": 5},
        "metric_value_kwargs_id": "n_rows=5",
    }
