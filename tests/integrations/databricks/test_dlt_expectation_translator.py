import pytest

from great_expectations.core import ExpectationConfiguration
from integrations.databricks.dlt_expectation_translator import (
    translate_expectation_config_to_dlt_expectation,
)
from integrations.databricks.exceptions import UnsupportedExpectationConfiguration


@pytest.fixture
def expect_column_values_to_not_be_null_config():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "a",
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def expect_column_values_to_be_between_config():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "a",
            "min_value": 10,
            "max_value": 25,
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def expect_column_values_to_be_between_strict_config():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "a",
            "min_value": 10,
            "max_value": 25,
            "strict_min": True,
            "strict_max": True,
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def expect_column_values_to_be_null_config():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_null",
        kwargs={
            "column": "a",
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is an expectation."},
    )


# TODO: Parametrize fixtures and run for each configuration type
def test_translate_expectation_config_to_dlt_expectation(
    expect_column_values_to_not_be_null_config,
    expect_column_values_to_be_between_config,
    expect_column_values_to_be_between_strict_config,
):

    expect_column_values_to_not_be_null_config_dlt = [
        ("my_not_null_config_col_a", "a IS NOT NULL")
    ]
    assert (
        translate_expectation_config_to_dlt_expectation(
            expectation_configuration=expect_column_values_to_not_be_null_config,
            dlt_expectation_name="my_not_null_config_col_a",
        )
        == expect_column_values_to_not_be_null_config_dlt
    )

    expect_column_values_to_be_between_config_dlt = [
        ("my_between_config_col_a_1", "a >= 10"),
        ("my_between_config_col_a_2", "a <= 25"),
    ]
    assert (
        translate_expectation_config_to_dlt_expectation(
            expectation_configuration=expect_column_values_to_be_between_config,
            dlt_expectation_name="my_between_config_col_a",
        )
        == expect_column_values_to_be_between_config_dlt
    )

    expect_column_values_to_be_between_strict_config_dlt = [
        ("my_between_config_col_a_1", "a > 10"),
        ("my_between_config_col_a_2", "a < 25"),
    ]
    assert (
        translate_expectation_config_to_dlt_expectation(
            expectation_configuration=expect_column_values_to_be_between_strict_config,
            dlt_expectation_name="my_between_config_col_a",
        )
        == expect_column_values_to_be_between_strict_config_dlt
    )


def test_unsupported_expectation(expect_column_values_to_be_null_config):

    with pytest.raises(UnsupportedExpectationConfiguration):
        translate_expectation_config_to_dlt_expectation(
            expectation_configuration=expect_column_values_to_be_null_config,
            dlt_expectation_name="dlt_expectation_name",
        )
