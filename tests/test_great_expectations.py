import os
import random
import re
import unittest

import pandas as pd
import pytest
from freezegun import freeze_time

import great_expectations as ge
from great_expectations.core import (
    ExpectationConfiguration,
    expectationSuiteSchema,
    expectationSuiteValidationResultSchema,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_asset.data_asset import (
    ValidationStatistics,
    _calc_validation_statistics,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset import MetaPandasDataset, PandasDataset
from great_expectations.exceptions import InvalidCacheValueError

try:
    from unittest import mock
except ImportError:
    from unittest import mock


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
    for x in range(3, int(n ** 0.5) + 1, 2):
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
        unexpected_values = list(self[column][not_null][result == False])

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
                    "unexpected_index_list": list(self.index[result == False])[:20],
                },
            }
        else:
            return {
                "success": len(unexpected_values) == 0,
                "result": {
                    "unexpected_list": unexpected_values[:20],
                    "unexpected_index_list": list(self.index[result == False])[:20],
                },
            }


def test_custom_class():
    script_path = os.path.dirname(os.path.realpath(__file__))
    df = ge.read_csv(
        script_path + "/test_sets/Titanic.csv", dataset_class=CustomPandasDataset
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    assert df.expect_column_values_to_be_prime("Age").result["unexpected_list"] == [
        30.0,
        25.0,
        0.92000000000000004,
        63.0,
        39.0,
        58.0,
        50.0,
        24.0,
        36.0,
        26.0,
        25.0,
        25.0,
        28.0,
        45.0,
        39.0,
        30.0,
        58.0,
        45.0,
        22.0,
        48.0,
        44.0,
        60.0,
        45.0,
        58.0,
        36.0,
        33.0,
        36.0,
        36.0,
        14.0,
        49.0,
        36.0,
        46.0,
        27.0,
        27.0,
        26.0,
        64.0,
        39.0,
        55.0,
        70.0,
        69.0,
        36.0,
        39.0,
        38.0,
        27.0,
        27.0,
        4.0,
        27.0,
        50.0,
        48.0,
        49.0,
        48.0,
        39.0,
        36.0,
        30.0,
        24.0,
        28.0,
        64.0,
        60.0,
        49.0,
        44.0,
        22.0,
        60.0,
        48.0,
        35.0,
        22.0,
        45.0,
        49.0,
        54.0,
        38.0,
        58.0,
        45.0,
        46.0,
        25.0,
        21.0,
        48.0,
        49.0,
        45.0,
        36.0,
        55.0,
        52.0,
        24.0,
        16.0,
        44.0,
        51.0,
        42.0,
        35.0,
        35.0,
        38.0,
        35.0,
        50.0,
        49.0,
        46.0,
        58.0,
        42.0,
        40.0,
        42.0,
        55.0,
        50.0,
        16.0,
        21.0,
        30.0,
        15.0,
        30.0,
        46.0,
        54.0,
        36.0,
        28.0,
        65.0,
        33.0,
        44.0,
        55.0,
        36.0,
        58.0,
        64.0,
        64.0,
        22.0,
        28.0,
        22.0,
        18.0,
        52.0,
        46.0,
        56.0,
        33.0,
        27.0,
        55.0,
        54.0,
        48.0,
        18.0,
        21.0,
        34.0,
        40.0,
        36.0,
        50.0,
        39.0,
        56.0,
        28.0,
        56.0,
        56.0,
        24.0,
        18.0,
        24.0,
        45.0,
        40.0,
        6.0,
        57.0,
        32.0,
        62.0,
        54.0,
        52.0,
        62.0,
        63.0,
        46.0,
        52.0,
        39.0,
        18.0,
        48.0,
        49.0,
        39.0,
        46.0,
        64.0,
        60.0,
        60.0,
        55.0,
        54.0,
        21.0,
        57.0,
        45.0,
        50.0,
        50.0,
        27.0,
        20.0,
        51.0,
        21.0,
        36.0,
        40.0,
        32.0,
        33.0,
        30.0,
        28.0,
        18.0,
        34.0,
        32.0,
        57.0,
        18.0,
        36.0,
        28.0,
        51.0,
        32.0,
        28.0,
        36.0,
        4.0,
        1.0,
        12.0,
        34.0,
        26.0,
        27.0,
        15.0,
        45.0,
        40.0,
        20.0,
        25.0,
        36.0,
        25.0,
        42.0,
        26.0,
        26.0,
        0.82999999999999996,
        54.0,
        44.0,
        52.0,
        30.0,
        30.0,
        27.0,
        24.0,
        35.0,
        8.0,
        22.0,
        30.0,
        20.0,
        21.0,
        49.0,
        8.0,
        28.0,
        18.0,
        28.0,
        22.0,
        25.0,
        18.0,
        32.0,
        18.0,
        42.0,
        34.0,
        8.0,
        21.0,
        38.0,
        38.0,
        35.0,
        35.0,
        38.0,
        24.0,
        16.0,
        26.0,
        45.0,
        24.0,
        21.0,
        22.0,
        34.0,
        30.0,
        50.0,
        30.0,
        1.0,
        44.0,
        28.0,
        6.0,
        30.0,
        45.0,
        24.0,
        24.0,
        49.0,
        48.0,
        34.0,
        32.0,
        21.0,
        18.0,
        21.0,
        52.0,
        42.0,
        36.0,
        21.0,
        33.0,
        34.0,
        22.0,
        45.0,
        30.0,
        26.0,
        34.0,
        26.0,
        22.0,
        1.0,
        25.0,
        48.0,
        57.0,
        27.0,
        30.0,
        20.0,
        45.0,
        46.0,
        30.0,
        48.0,
        54.0,
        64.0,
        32.0,
        18.0,
        32.0,
        26.0,
        20.0,
        39.0,
        22.0,
        24.0,
        28.0,
        50.0,
        20.0,
        40.0,
        42.0,
        21.0,
        32.0,
        34.0,
        33.0,
        8.0,
        36.0,
        34.0,
        30.0,
        28.0,
        0.80000000000000004,
        25.0,
        50.0,
        21.0,
        25.0,
        18.0,
        20.0,
        30.0,
        30.0,
        35.0,
        22.0,
        25.0,
        25.0,
        14.0,
        50.0,
        22.0,
        27.0,
        27.0,
        30.0,
        22.0,
        35.0,
        30.0,
        28.0,
        12.0,
        40.0,
        36.0,
        28.0,
        32.0,
        4.0,
        36.0,
        33.0,
        32.0,
        26.0,
        30.0,
        24.0,
        18.0,
        42.0,
        16.0,
        35.0,
        16.0,
        25.0,
        18.0,
        20.0,
        30.0,
        26.0,
        40.0,
        24.0,
        18.0,
        0.82999999999999996,
        20.0,
        25.0,
        35.0,
        32.0,
        20.0,
        39.0,
        39.0,
        6.0,
        38.0,
        9.0,
        26.0,
        4.0,
        20.0,
        26.0,
        25.0,
        18.0,
        24.0,
        35.0,
        40.0,
        38.0,
        9.0,
        45.0,
        27.0,
        20.0,
        32.0,
        33.0,
        18.0,
        40.0,
        26.0,
        15.0,
        45.0,
        18.0,
        27.0,
        22.0,
        26.0,
        22.0,
        20.0,
        32.0,
        21.0,
        18.0,
        26.0,
        6.0,
        9.0,
        40.0,
        32.0,
        26.0,
        18.0,
        20.0,
        22.0,
        22.0,
        35.0,
        21.0,
        20.0,
        18.0,
        18.0,
        38.0,
        30.0,
        21.0,
        21.0,
        21.0,
        24.0,
        33.0,
        33.0,
        28.0,
        16.0,
        28.0,
        24.0,
        21.0,
        32.0,
        26.0,
        18.0,
        20.0,
        24.0,
        24.0,
        36.0,
        30.0,
        22.0,
        35.0,
        27.0,
        30.0,
        36.0,
        9.0,
        44.0,
        45.0,
        22.0,
        30.0,
        34.0,
        28.0,
        0.33000000000000002,
        27.0,
        25.0,
        24.0,
        22.0,
        21.0,
        26.0,
        33.0,
        1.0,
        0.17000000000000001,
        25.0,
        36.0,
        36.0,
        30.0,
        26.0,
        65.0,
        42.0,
        32.0,
        30.0,
        24.0,
        24.0,
        24.0,
        22.0,
        18.0,
        16.0,
        45.0,
        21.0,
        18.0,
        9.0,
        48.0,
        16.0,
        25.0,
        38.0,
        22.0,
        16.0,
        33.0,
        9.0,
        38.0,
        40.0,
        14.0,
        16.0,
        9.0,
        10.0,
        6.0,
        40.0,
        32.0,
        20.0,
        28.0,
        24.0,
        28.0,
        24.0,
        20.0,
        45.0,
        26.0,
        21.0,
        27.0,
        18.0,
        26.0,
        22.0,
        28.0,
        22.0,
        27.0,
        42.0,
        27.0,
        25.0,
        27.0,
        20.0,
        48.0,
        34.0,
        22.0,
        33.0,
        32.0,
        26.0,
        49.0,
        1.0,
        33.0,
        4.0,
        24.0,
        32.0,
        27.0,
        21.0,
        32.0,
        20.0,
        21.0,
        30.0,
        21.0,
        22.0,
        4.0,
        39.0,
        20.0,
        21.0,
        44.0,
        42.0,
        21.0,
        24.0,
        25.0,
        22.0,
        22.0,
        39.0,
        26.0,
        4.0,
        22.0,
        26.0,
        1.5,
        36.0,
        18.0,
        25.0,
        22.0,
        20.0,
        26.0,
        22.0,
        32.0,
        21.0,
        21.0,
        36.0,
        39.0,
        25.0,
        45.0,
        36.0,
        30.0,
        20.0,
        21.0,
        1.5,
        25.0,
        18.0,
        63.0,
        18.0,
        15.0,
        28.0,
        36.0,
        28.0,
        10.0,
        36.0,
        30.0,
        22.0,
        14.0,
        22.0,
        51.0,
        18.0,
        45.0,
        28.0,
        21.0,
        27.0,
        36.0,
        27.0,
        15.0,
        27.0,
        26.0,
        22.0,
        24.0,
    ]

    primes = [3, 5, 7, 11, 13, 17, 23, 31]
    df["primes"] = df.Age.map(lambda x: random.choice(primes))
    assert df.expect_column_values_to_be_prime("primes").result["unexpected_list"] == []


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


@freeze_time("11/05/1955")
def test_validate():
    with open(
        file_relative_path(__file__, "./test_sets/titanic_expectations.json")
    ) as f:
        my_expectation_suite = expectationSuiteSchema.loads(f.read())

    with mock.patch("uuid.uuid1") as uuid:
        uuid.return_value = "1234"
        my_df = ge.read_csv(
            file_relative_path(__file__, "./test_sets/Titanic.csv"),
            expectation_suite=my_expectation_suite,
        )
    my_df.set_default_expectation_argument("result_format", "COMPLETE")

    results = my_df.validate(catch_exceptions=False)

    with open(
        file_relative_path(
            __file__, "./test_sets/titanic_expected_data_asset_validate_results.json"
        )
    ) as f:
        expected_results = expectationSuiteValidationResultSchema.loads(f.read())

    del results.meta["great_expectations_version"]

    assert results.to_json_dict() == expected_results.to_json_dict()

    # Now, change the results and ensure they are no longer equal
    results.results[0] = ExpectationValidationResult()
    assert results.to_json_dict() != expected_results.to_json_dict()

    # Finally, confirm that only_return_failures works
    # and does not affect the "statistics" field.
    validation_results = my_df.validate(only_return_failures=True)
    del validation_results.meta["great_expectations_version"]

    expected_results = ExpectationSuiteValidationResult(
        meta={
            "expectation_suite_name": "titanic",
            "run_id": {"run_name": None, "run_time": "1955-11-05T00:00:00+00:00"},
            "validation_time": "19551105T000000.000000Z",
            "batch_kwargs": {"ge_batch_id": "1234"},
            "batch_markers": {},
            "batch_parameters": {},
        },
        results=[
            ExpectationValidationResult(
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={"column": "PClass", "value_set": ["1st", "2nd", "3rd"]},
                ),
                success=False,
                exception_info={
                    "exception_message": None,
                    "exception_traceback": None,
                    "raised_exception": False,
                },
                result={
                    "partial_unexpected_index_list": [456],
                    "unexpected_count": 1,
                    "unexpected_list": ["*"],
                    "unexpected_percent": 0.07616146230007616,
                    "element_count": 1313,
                    "missing_percent": 0.0,
                    "partial_unexpected_counts": [{"count": 1, "value": "*"}],
                    "partial_unexpected_list": ["*"],
                    "unexpected_percent_total": 0.07616146230007616,
                    "unexpected_percent_nonmissing": 0.07616146230007616,
                    "missing_count": 0,
                    "unexpected_index_list": [456],
                },
            )
        ],
        success=expected_results.success,  # unaffected
        statistics=expected_results["statistics"],  # unaffected
    )
    assert validation_results.to_json_dict() == expected_results.to_json_dict()


@mock.patch(
    "great_expectations.core.ExpectationValidationResult.validate_result_dict",
    return_value=False,
)
def test_validate_with_invalid_result_catch_exceptions_false(validate_result_dict):
    with open(
        file_relative_path(__file__, "./test_sets/titanic_expectations.json")
    ) as f:
        my_expectation_suite = expectationSuiteSchema.loads(f.read())

    with mock.patch("uuid.uuid1") as uuid:
        uuid.return_value = "1234"
        my_df = ge.read_csv(
            file_relative_path(__file__, "./test_sets/Titanic.csv"),
            expectation_suite=my_expectation_suite,
        )
    my_df.set_default_expectation_argument("result_format", "COMPLETE")

    with pytest.raises(InvalidCacheValueError):
        with pytest.warns(Warning, match=r"No great_expectations version found"):
            my_df.validate(catch_exceptions=False)


@freeze_time("11/05/1955")
@mock.patch(
    "great_expectations.core.ExpectationValidationResult.validate_result_dict",
    return_value=False,
)
def test_validate_with_invalid_result(validate_result_dict):
    with open(
        file_relative_path(__file__, "./test_sets/titanic_expectations.json")
    ) as f:
        my_expectation_suite = expectationSuiteSchema.loads(f.read())

    with mock.patch("uuid.uuid1") as uuid:
        uuid.return_value = "1234"
        my_df = ge.read_csv(
            file_relative_path(__file__, "./test_sets/Titanic.csv"),
            expectation_suite=my_expectation_suite,
        )
    my_df.set_default_expectation_argument("result_format", "COMPLETE")

    results = my_df.validate()  # catch_exceptions=True is default

    with open(
        file_relative_path(
            __file__,
            "./test_sets/titanic_expected_data_asset_validate_results_with_exceptions.json",
        )
    ) as f:
        expected_results = expectationSuiteValidationResultSchema.loads(f.read())

    del results.meta["great_expectations_version"]

    for result in results.results:
        result.exception_info.pop("exception_traceback")

    assert results.to_json_dict() == expected_results.to_json_dict()


def test_validate_catch_non_existent_expectation():
    df = ge.dataset.PandasDataset({"x": [1, 2, 3, 4, 5]})

    validation_config_non_existent_expectation = ExpectationSuite(
        expectation_suite_name="default",
        meta={"great_expectations_version": ge.__version__},
        expectations=[
            ExpectationConfiguration(
                expectation_type="non_existent_expectation", kwargs={"column": "x"}
            )
        ],
    )

    results = df.validate(expectation_suite=validation_config_non_existent_expectation)

    assert (
        "object has no attribute 'non_existent_expectation'"
        in results.results[0].exception_info["exception_message"]
    )


def test_validate_catch_invalid_parameter():
    df = ge.dataset.PandasDataset({"x": [1, 2, 3, 4, 5]})

    validation_config_invalid_parameter = ExpectationSuite(
        expectation_suite_name="default",
        meta={"great_expectations_version": ge.__version__},
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "x", "min_value": 6, "max_value": 5},
            )
        ],
    )

    result = df.validate(expectation_suite=validation_config_invalid_parameter)
    assert (
        "min_value cannot be greater than max_value"
        in result.results[0].exception_info["exception_message"]
    )


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


def test_stats_mixed_expectations():
    expectation_results = [
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=True),
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(2, 1, 1, 50.0, False)
    assert expected == actual


def test_generate_library_json_from_registered_expectations():
    library_json = ge.util.generate_library_json_from_registered_expectations()
    assert len(library_json) > 50


class TestIO(unittest.TestCase):
    def test_read_csv(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_csv(
            script_path + "/test_sets/Titanic.csv",
        )

    def test_read_json(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_json(
            script_path + "/test_sets/test_json_data_file.json",
        )
        assert df["x"][0] == "i"
        assert isinstance(df, PandasDataset)
        assert sorted(list(df.keys())) == ["x", "y", "z"]

        df = ge.read_json(
            script_path + "/test_sets/nested_test_json_data_file.json",
            accessor_func=lambda x: x["data"],
        )
        assert df["x"][0] == "i"
        assert isinstance(df, PandasDataset)
        assert sorted(list(df.keys())) == ["x", "y", "z"]

    def test_read_excel(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_excel(
            script_path + "/test_sets/Titanic_multi_sheet.xlsx",
        )
        assert df["Name"][0] == "Allen, Miss Elisabeth Walton"
        assert isinstance(df, PandasDataset)

        # Note that pandas changed the parameter name from sheetname to sheet_name.
        # We will test with both options to ensure that the versions are correct.
        pandas_version = pd.__version__
        if re.match(r"0\.2[012]\.", pandas_version) is not None:
            dfs_dict = ge.read_excel(
                script_path + "/test_sets/Titanic_multi_sheet.xlsx", sheetname=None
            )

        else:
            dfs_dict = ge.read_excel(
                script_path + "/test_sets/Titanic_multi_sheet.xlsx", sheet_name=None
            )
        assert isinstance(dfs_dict, dict)
        assert list(dfs_dict.keys()) == ["Titanic_1", "Titanic_2", "Titanic_3"]
        assert isinstance(dfs_dict["Titanic_1"], PandasDataset)
        assert dfs_dict["Titanic_1"]["Name"][0] == "Allen, Miss Elisabeth Walton"

    def test_read_table(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_table(script_path + "/test_sets/Titanic.csv", sep=",")
        assert df["Name"][0] == "Allen, Miss Elisabeth Walton"
        assert isinstance(df, PandasDataset)

    def test_read_feather(self):
        pandas_version = re.match(r"(\d+)\.(\d+)\..+", pd.__version__)
        if pandas_version is None:
            raise ValueError("Unrecognized pandas version!")
        else:
            pandas_major_version = int(pandas_version.group(1))
            pandas_minor_version = int(pandas_version.group(2))
            if pandas_major_version == 0 and pandas_minor_version < 25:
                pytest.skip("Skipping because of old pandas version.")

        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_feather(script_path + "/test_sets/Titanic.feather")
        assert df["Name"][0] == "Allen, Miss Elisabeth Walton"
        assert isinstance(df, PandasDataset)

    def test_read_parquet(self):
        """
        This test is unusual, because on travis (but only on travis), we have observed problems importing pyarrow,
        which breaks this test (since it requires pyarrow available).

        The issue seems to be related to a binary compatibility issue with the installed/available version of numpy:
        pyarrow 0.10 requires numpy >= 1.14.

        Since pyarrow is not in our actual requirements, we are not going to adjust up the required numpy version.
        """

        # Pass this test if the available version of pandas is less than 0.21.0, because prior
        # versions of pandas did not include the read_parquet function.
        pandas_version = re.match(r"(\d+)\.(\d+)\..+", pd.__version__)
        if pandas_version is None:
            raise ValueError("Unrecognized pandas version!")
        else:
            pandas_major_version = int(pandas_version.group(1))
            pandas_minor_version = int(pandas_version.group(2))
            if pandas_major_version == 0 and pandas_minor_version < 23:
                pytest.skip("Pandas version < 23 is no longer compatible with pyarrow")

        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_parquet(script_path + "/test_sets/Titanic.parquet")
        assert df["Name"][1] == "Allen, Miss Elisabeth Walton"
        assert isinstance(df, PandasDataset)

    def test_read_pickle(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_pickle(
            script_path + "/test_sets/Titanic.pkl",
        )
        assert df["Name"][0] == "Allen, Miss Elisabeth Walton"
        assert isinstance(df, PandasDataset)


if __name__ == "__main__":
    unittest.main()
