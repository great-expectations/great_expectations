import itertools
from typing import Any, Dict

import pytest

import great_expectations.core.expectation_configuration as expectation_configuration
import great_expectations.expectations.expectation as expectation
from great_expectations.exceptions import InvalidExpectationConfigurationError


class FakeMulticolumnExpectation(expectation.MulticolumnMapExpectation):
    map_metric = "fake_multicol_metric"


class FakeColumnMapExpectation(expectation.ColumnMapExpectation):
    map_metric = "fake_col_metric"


class FakeColumnPairMapExpectation(expectation.ColumnPairMapExpectation):
    map_metric = "fake_pair_metric"


def fake_config(
    expectation_type: str, config_kwargs: Dict[str, Any]
) -> expectation_configuration.ExpectationConfiguration:
    return expectation_configuration.ExpectationConfiguration(
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
    except:
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
    except:
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
        fake_expectation = fake_expectation_cls(config)
