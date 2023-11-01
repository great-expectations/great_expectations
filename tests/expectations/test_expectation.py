import itertools
from typing import Any, Dict

import pytest

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.expectations import expectation
from great_expectations.validator.metric_configuration import MetricConfiguration


class FakeMulticolumnExpectation(expectation.MulticolumnMapExpectation):
    map_metric = "fake_multicol_metric"


class FakeColumnMapExpectation(expectation.ColumnMapExpectation):
    map_metric = "fake_col_metric"


class FakeColumnPairMapExpectation(expectation.ColumnPairMapExpectation):
    map_metric = "fake_pair_metric"


@pytest.fixture
def metric_config_exists():
    temp_metrics_config = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={
            "batch_id": "projects-projects",
            "table": None,
            "column": "name",
            "row_condition": None,
            "condition_parser": None,
        },
        metric_value_kwargs={
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
                "include_unexpected_rows": False,
            }
        },
    )
    return [temp_metrics_config]


@pytest.fixture
def metric_config_does_not_exist():
    temp_metrics_config = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={
            "batch_id": "projects-projects",
            "table": None,
            "column": None,
            "row_condition": None,
            "condition_parser": None,
        },
        metric_value_kwargs={
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
                "include_unexpected_rows": False,
            }
        },
    )
    return [temp_metrics_config]


@pytest.fixture
def metrics_dict():
    return {
        (
            "table.row_count",
            "9fb963653447edb4ae8538917d6915bc",
            "result_format={'result_format': 'BASIC', 'partial_unexpected_count': 20, 'include_unexpected_rows': False}",
        ): "i_exist"
    }


@pytest.fixture
def real_expectation_configuration():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "name", "batch_id": "projects-projects"},
        meta={},
    )


@pytest.fixture
def fake_expectation_configuration():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "I_dont_exist_either", "batch_id": "projects-projects"},
        meta={},
    )


def fake_config(
    expectation_type: str, config_kwargs: Dict[str, Any]
) -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs=config_kwargs,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    [
        (
            FakeMulticolumnExpectation,
            fake_config("fake_multicolumn_expectation", {"column_list": []}),
        ),
        (
            FakeColumnMapExpectation,
            fake_config("fake_column_map_expectation", {"column": "col"}),
        ),
        (
            FakeColumnPairMapExpectation,
            fake_config(
                "fake_column_pair_map_expectation",
                {"column_A": "colA", "column_B": "colB"},
            ),
        ),
    ],
)
def test_multicolumn_expectation_has_default_mostly(fake_expectation_cls, config):
    try:
        fake_expectation = fake_expectation_cls(config)
    except Exception:
        assert (
            False
        ), "Validate configuration threw an error when testing default mostly value"
    assert (
        fake_expectation.get_success_kwargs().get("mostly") == 1
    ), "Default mostly success ratio is not 1"


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    itertools.chain(
        *[
            [
                (
                    FakeMulticolumnExpectation,
                    fake_config(
                        "fake_multicolumn_expectation", {"column_list": [], "mostly": x}
                    ),
                )
                for x in [0, 0.5, 1]
            ],
            [
                (
                    FakeColumnMapExpectation,
                    fake_config(
                        "fake_column_map_expectation", {"column": "col", "mostly": x}
                    ),
                )
                for x in [0, 0.5, 1]
            ],
            [
                (
                    FakeColumnPairMapExpectation,
                    fake_config(
                        "fake_column_pair_map_expectation",
                        {"column_A": "colA", "column_B": "colB", "mostly": x},
                    ),
                )
                for x in [0, 0.5, 1]
            ],
        ]
    ),
)
def test_expectation_succeeds_with_valid_mostly(fake_expectation_cls, config):
    try:
        fake_expectation = fake_expectation_cls(config)
    except Exception:
        assert (
            False
        ), "Validate configuration threw an error when testing default mostly value"
    assert (
        fake_expectation.get_success_kwargs().get("mostly") == config.kwargs["mostly"]
    ), "Default mostly success ratio is not 1"


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    [
        (
            FakeMulticolumnExpectation,
            fake_config(
                "fake_multicolumn_expectation", {"column_list": [], "mostly": -0.5}
            ),
        ),
        (
            FakeColumnMapExpectation,
            fake_config(
                "fake_column_map_expectation", {"column": "col", "mostly": 1.5}
            ),
        ),
        (
            FakeColumnPairMapExpectation,
            fake_config(
                "fake_column_pair_map_expectation",
                {"column_A": "colA", "column_B": "colB", "mostly": -1},
            ),
        ),
    ],
)
def test_multicolumn_expectation_validation_errors_with_bad_mostly(
    fake_expectation_cls, config
):
    with pytest.raises(InvalidExpectationConfigurationError):
        fake_expectation_cls(config)


@pytest.mark.unit
def test_validate_dependencies_against_available_metrics(
    metric_config_exists,
    metric_config_does_not_exist,
    metrics_dict,
    real_expectation_configuration,
    fake_expectation_configuration,
):
    expectation._validate_dependencies_against_available_metrics(
        validation_dependencies=metric_config_exists,
        metrics=metrics_dict,
        configuration=real_expectation_configuration,
    )

    with pytest.raises(InvalidExpectationConfigurationError):
        expectation._validate_dependencies_against_available_metrics(
            validation_dependencies=metric_config_does_not_exist,
            metrics=metrics_dict,
            configuration=fake_expectation_configuration,
        )
