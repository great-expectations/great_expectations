import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.core import (
    ExpectationConfiguration,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_asset.data_asset import (
    ValidationStatistics,
    _calc_validation_statistics,
)
from great_expectations.dataset import MetaPandasDataset, PandasDataset


def isprime(n):
    # https://stackoverflow.com/questions/18833759/python-prime-number-checker
    """check if integer n is a prime"""

    # make sure n is a positive integer
    n = abs(int(n))

    # 0 and 1 are not primes
    if n < 2:
        return False

    # 2 is the only even prime number
    if n == 2:
        return True

    # all other even numbers are not primes
    if not n & 1:
        return False

    # range starts with 3 and only needs to go up
    # the square root of n for all odd numbers
    for x in range(3, int(n**0.5) + 1, 2):
        if n % x == 0:
            return False

    return True


class CustomPandasDataset(PandasDataset):
    @MetaPandasDataset.column_map_expectation
    def expect_column_values_to_be_prime(self, column):
        return column.map(isprime)

    @MetaPandasDataset.expectation(["column", "mostly"])
    def expect_column_values_to_equal_1(self, column, mostly=None):
        not_null = self[column].notnull()

        result = self[column][not_null] == 1
        unexpected_values = list(self[column][not_null][result == False])  # noqa: E712

        if mostly:
            # Prevent division-by-zero errors
            if len(not_null) == 0:
                return {
                    "success": True,
                    "result": {
                        "unexpected_list": unexpected_values,
                        "unexpected_index_list": self.index[result],
                    },
                }

            percent_equaling_1 = float(sum(result)) / len(not_null)
            return {
                "success": percent_equaling_1 >= mostly,
                "result": {
                    "unexpected_list": unexpected_values[:20],
                    "unexpected_index_list": list(
                        self.index[result == False]  # noqa: E712
                    )[:20],
                },
            }
        else:
            return {
                "success": len(unexpected_values) == 0,
                "result": {
                    "unexpected_list": unexpected_values[:20],
                    "unexpected_index_list": list(
                        self.index[result == False]  # noqa: E712
                    )[:20],
                },
            }


@pytest.mark.unit
def test_custom_expectation():
    df = CustomPandasDataset({"x": [1, 1, 1, 1, 2]})
    df.set_default_expectation_argument("result_format", "COMPLETE")
    result = df.expect_column_values_to_be_prime("x")

    assert result.success is False
    assert result.result["unexpected_index_list"] == [0, 1, 2, 3]
    assert result.result["unexpected_list"] == [1, 1, 1, 1]

    result = df.expect_column_values_to_equal_1("x", mostly=0.8)
    assert result.success is True
    assert result.result["unexpected_index_list"] == [4]
    assert result.result["unexpected_list"] == [2]


# Ensure that Custom Data Set classes can properly call non-overridden methods from their parent class
@pytest.mark.unit
def test_base_class_expectation():
    df = CustomPandasDataset(
        {
            "aaa": [1, 2, 3, 4, 5],
            "bbb": [10, 20, 30, 40, 50],
            "ccc": [9, 10, 11, 12, 13],
        }
    )

    assert (
        df.expect_column_values_to_be_between("aaa", min_value=1, max_value=5).success
        is True
    )


@pytest.mark.filesystem
def test_validate_catch_non_existent_expectation(empty_data_context):
    context: DataContext = empty_data_context
    df = gx.dataset.PandasDataset({"x": [1, 2, 3, 4, 5]})

    validation_config_non_existent_expectation = ExpectationSuite(
        expectation_suite_name="default",
        meta={"great_expectations_version": gx.__version__},
        expectations=[
            ExpectationConfiguration(
                expectation_type="non_existent_expectation", kwargs={"column": "x"}
            )
        ],
        data_context=context,
    )

    results = df.validate(expectation_suite=validation_config_non_existent_expectation)

    assert (
        "object has no attribute 'non_existent_expectation'"
        in results.results[0].exception_info["exception_message"]
    )


@pytest.mark.filesystem
def test_validate_catch_invalid_parameter(empty_data_context):
    context: DataContext = empty_data_context
    df = gx.dataset.PandasDataset({"x": [1, 2, 3, 4, 5]})

    validation_config_invalid_parameter = ExpectationSuite(
        expectation_suite_name="default",
        meta={"great_expectations_version": gx.__version__},
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "x", "min_value": 6, "max_value": 5},
            )
        ],
        data_context=context,
    )

    result = df.validate(expectation_suite=validation_config_invalid_parameter)
    assert (
        "min_value cannot be greater than max_value"
        in result.results[0].exception_info["exception_message"]
    )


@pytest.mark.unit
def test_stats_no_expectations():
    expectation_results = []
    actual = _calc_validation_statistics(expectation_results)

    # pay attention to these two
    assert None is actual.success_percent
    assert True is actual.success
    # the rest is boring
    assert 0 == actual.successful_expectations
    assert 0 == actual.evaluated_expectations
    assert 0 == actual.unsuccessful_expectations


@pytest.mark.unit
def test_stats_no_successful_expectations():
    expectation_results = [ExpectationValidationResult(success=False)]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(1, 0, 1, 0.0, False)
    assert expected == actual

    expectation_results = [
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=False),
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(3, 0, 3, 0.0, False)
    assert expected == actual


@pytest.mark.unit
def test_stats_all_successful_expectations():
    expectation_results = [
        ExpectationValidationResult(success=True),
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(1, 1, 0, 100.0, True)
    assert expected == actual

    expectation_results = [
        ExpectationValidationResult(success=True),
        ExpectationValidationResult(success=True),
        ExpectationValidationResult(success=True),
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(3, 3, 0, 100.0, True)
    assert expected == actual


@pytest.mark.unit
def test_stats_mixed_expectations():
    expectation_results = [
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=True),
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(2, 1, 1, 50.0, False)
    assert expected == actual
