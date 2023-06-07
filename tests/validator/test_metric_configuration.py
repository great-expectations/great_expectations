import pytest

from great_expectations.core import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes
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


@pytest.mark.unit
def test_metric_configuration_get_domain_type(
    table_head_metric_config: MetricConfiguration,
    column_histogram_metric_config: MetricConfiguration,
) -> None:
    assert table_head_metric_config.get_domain_type() == MetricDomainTypes.TABLE
    assert column_histogram_metric_config.get_domain_type() == MetricDomainTypes.COLUMN


@pytest.mark.unit
def test_metric_configuration_get_domain(
    table_head_metric_config: MetricConfiguration,
    column_histogram_metric_config: MetricConfiguration,
) -> None:
    assert table_head_metric_config.get_domain() == Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs={},
    )
    assert column_histogram_metric_config.get_domain() == Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "my_column",
        },
    )
