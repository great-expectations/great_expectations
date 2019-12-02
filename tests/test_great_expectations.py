import json
import os
import random
import unittest
from datetime import datetime

from great_expectations.core import ExpectationValidationResult, ExpectationSuiteValidationResult, \
    ExpectationConfiguration, expectationSuiteSchema, expectationSuiteValidationResultSchema, ExpectationSuite

try:
    from unittest import mock
except ImportError:
    import mock

from six import PY2
import pandas as pd
import re

import great_expectations as ge
from great_expectations.dataset import PandasDataset, MetaPandasDataset
from great_expectations.data_asset.data_asset import (
    _calc_validation_statistics,
    ValidationStatistics,
)
from .test_utils import assertDeepAlmostEqual


def isprime(n):
    # https://stackoverflow.com/questions/18833759/python-prime-number-checker
    '''check if integer n is a prime'''

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
        unexpected_values = list(self[column][not_null][result == False])

        if mostly:
            # Prevent division-by-zero errors
            if len(not_null) == 0:
                return {
                    'success': True,
                    'result': {
                        'unexpected_list': unexpected_values,
                        'unexpected_index_list': self.index[result],
                    }
                }

            percent_equaling_1 = float(sum(result))/len(not_null)
            return {
                "success": percent_equaling_1 >= mostly,
                'result': {
                    "unexpected_list": unexpected_values[:20],
                    "unexpected_index_list": list(self.index[result == False])[:20],
                }
            }
        else:
            return {
                "success": len(unexpected_values) == 0,
                'result': {
                    "unexpected_list": unexpected_values[:20],
                    "unexpected_index_list": list(self.index[result == False])[:20],
                }
            }


def test_custom_class():
    script_path = os.path.dirname(os.path.realpath(__file__))
    df = ge.read_csv(
        script_path+'/test_sets/Titanic.csv',
        dataset_class=CustomPandasDataset
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    assert df.expect_column_values_to_be_prime(
            'Age').result['unexpected_list'] == \
        [30.0, 25.0, 0.92000000000000004, 63.0, 39.0, 58.0, 50.0, 24.0, 36.0, 26.0, 25.0, 25.0, 28.0, 45.0, 39.0,
         30.0, 58.0, 45.0, 22.0, 48.0, 44.0, 60.0, 45.0, 58.0, 36.0, 33.0, 36.0, 36.0, 14.0, 49.0, 36.0, 46.0, 27.0,
         27.0, 26.0, 64.0, 39.0, 55.0, 70.0, 69.0, 36.0, 39.0, 38.0, 27.0, 27.0, 4.0, 27.0, 50.0, 48.0, 49.0, 48.0,
         39.0, 36.0, 30.0, 24.0, 28.0, 64.0, 60.0, 49.0, 44.0, 22.0, 60.0, 48.0, 35.0, 22.0, 45.0, 49.0, 54.0, 38.0,
         58.0, 45.0, 46.0, 25.0, 21.0, 48.0, 49.0, 45.0, 36.0, 55.0, 52.0, 24.0, 16.0, 44.0, 51.0, 42.0, 35.0, 35.0,
         38.0, 35.0, 50.0, 49.0, 46.0, 58.0, 42.0, 40.0, 42.0, 55.0, 50.0, 16.0, 21.0, 30.0, 15.0, 30.0, 46.0, 54.0,
         36.0, 28.0, 65.0, 33.0, 44.0, 55.0, 36.0, 58.0, 64.0, 64.0, 22.0, 28.0, 22.0, 18.0, 52.0, 46.0, 56.0, 33.0,
         27.0, 55.0, 54.0, 48.0, 18.0, 21.0, 34.0, 40.0, 36.0, 50.0, 39.0, 56.0, 28.0, 56.0, 56.0, 24.0, 18.0, 24.0,
         45.0, 40.0, 6.0, 57.0, 32.0, 62.0, 54.0, 52.0, 62.0, 63.0, 46.0, 52.0, 39.0, 18.0, 48.0, 49.0, 39.0, 46.0,
         64.0, 60.0, 60.0, 55.0, 54.0, 21.0, 57.0, 45.0, 50.0, 50.0, 27.0, 20.0, 51.0, 21.0, 36.0, 40.0, 32.0, 33.0,
         30.0, 28.0, 18.0, 34.0, 32.0, 57.0, 18.0, 36.0, 28.0, 51.0, 32.0, 28.0, 36.0, 4.0, 1.0, 12.0, 34.0, 26.0,
         27.0, 15.0, 45.0, 40.0, 20.0, 25.0, 36.0, 25.0, 42.0, 26.0, 26.0, 0.82999999999999996, 54.0, 44.0, 52.0,
         30.0, 30.0, 27.0, 24.0, 35.0, 8.0, 22.0, 30.0, 20.0, 21.0, 49.0, 8.0, 28.0, 18.0, 28.0, 22.0, 25.0, 18.0,
         32.0, 18.0, 42.0, 34.0, 8.0, 21.0, 38.0, 38.0, 35.0, 35.0, 38.0, 24.0, 16.0, 26.0, 45.0, 24.0, 21.0, 22.0,
         34.0, 30.0, 50.0, 30.0, 1.0, 44.0, 28.0, 6.0, 30.0, 45.0, 24.0, 24.0, 49.0, 48.0, 34.0, 32.0, 21.0, 18.0,
         21.0, 52.0, 42.0, 36.0, 21.0, 33.0, 34.0, 22.0, 45.0, 30.0, 26.0, 34.0, 26.0, 22.0, 1.0, 25.0, 48.0, 57.0,
         27.0, 30.0, 20.0, 45.0, 46.0, 30.0, 48.0, 54.0, 64.0, 32.0, 18.0, 32.0, 26.0, 20.0, 39.0, 22.0, 24.0, 28.0,
         50.0, 20.0, 40.0, 42.0, 21.0, 32.0, 34.0, 33.0, 8.0, 36.0, 34.0, 30.0, 28.0, 0.80000000000000004, 25.0,
         50.0, 21.0, 25.0, 18.0, 20.0, 30.0, 30.0, 35.0, 22.0, 25.0, 25.0, 14.0, 50.0, 22.0, 27.0, 27.0, 30.0, 22.0,
         35.0, 30.0, 28.0, 12.0, 40.0, 36.0, 28.0, 32.0, 4.0, 36.0, 33.0, 32.0, 26.0, 30.0, 24.0, 18.0, 42.0, 16.0,
         35.0, 16.0, 25.0, 18.0, 20.0, 30.0, 26.0, 40.0, 24.0, 18.0, 0.82999999999999996, 20.0, 25.0, 35.0, 32.0,
         20.0, 39.0, 39.0, 6.0, 38.0, 9.0, 26.0, 4.0, 20.0, 26.0, 25.0, 18.0, 24.0, 35.0, 40.0, 38.0, 9.0, 45.0,
         27.0, 20.0, 32.0, 33.0, 18.0, 40.0, 26.0, 15.0, 45.0, 18.0, 27.0, 22.0, 26.0, 22.0, 20.0, 32.0, 21.0, 18.0,
         26.0, 6.0, 9.0, 40.0, 32.0, 26.0, 18.0, 20.0, 22.0, 22.0, 35.0, 21.0, 20.0, 18.0, 18.0, 38.0, 30.0, 21.0,
         21.0, 21.0, 24.0, 33.0, 33.0, 28.0, 16.0, 28.0, 24.0, 21.0, 32.0, 26.0, 18.0, 20.0, 24.0, 24.0, 36.0, 30.0,
         22.0, 35.0, 27.0, 30.0, 36.0, 9.0, 44.0, 45.0, 22.0, 30.0, 34.0, 28.0, 0.33000000000000002, 27.0, 25.0,
         24.0, 22.0, 21.0, 26.0, 33.0, 1.0, 0.17000000000000001, 25.0, 36.0, 36.0, 30.0, 26.0, 65.0, 42.0, 32.0,
         30.0, 24.0, 24.0, 24.0, 22.0, 18.0, 16.0, 45.0, 21.0, 18.0, 9.0, 48.0, 16.0, 25.0, 38.0, 22.0, 16.0, 33.0,
         9.0, 38.0, 40.0, 14.0, 16.0, 9.0, 10.0, 6.0, 40.0, 32.0, 20.0, 28.0, 24.0, 28.0, 24.0, 20.0, 45.0, 26.0,
         21.0, 27.0, 18.0, 26.0, 22.0, 28.0, 22.0, 27.0, 42.0, 27.0, 25.0, 27.0, 20.0, 48.0, 34.0, 22.0, 33.0, 32.0,
         26.0, 49.0, 1.0, 33.0, 4.0, 24.0, 32.0, 27.0, 21.0, 32.0, 20.0, 21.0, 30.0, 21.0, 22.0, 4.0, 39.0, 20.0,
         21.0, 44.0, 42.0, 21.0, 24.0, 25.0, 22.0, 22.0, 39.0, 26.0, 4.0, 22.0, 26.0, 1.5, 36.0, 18.0, 25.0, 22.0,
         20.0, 26.0, 22.0, 32.0, 21.0, 21.0, 36.0, 39.0, 25.0, 45.0, 36.0, 30.0, 20.0, 21.0, 1.5, 25.0, 18.0, 63.0,
         18.0, 15.0, 28.0, 36.0, 28.0, 10.0, 36.0, 30.0, 22.0, 14.0, 22.0, 51.0, 18.0, 45.0, 28.0, 21.0, 27.0, 36.0,
         27.0, 15.0, 27.0, 26.0, 22.0, 24.0]

    primes = [3, 5, 7, 11, 13, 17, 23, 31]
    df["primes"] = df.Age.map(lambda x: random.choice(primes))
    assert df.expect_column_values_to_be_prime("primes").result['unexpected_list'] == []


def test_custom_expectation():
    df = CustomPandasDataset({'x': [1, 1, 1, 1, 2]})
    df.set_default_expectation_argument("result_format", "COMPLETE")
    result = df.expect_column_values_to_be_prime('x')

    assert result.success is False
    assert result.result['unexpected_index_list'] == [0, 1, 2, 3]
    assert result.result['unexpected_list'] == [1, 1, 1, 1]

    result = df.expect_column_values_to_equal_1('x', mostly=.8)
    assert result.success is True
    assert result.result['unexpected_index_list'] == [4]
    assert result.result['unexpected_list'] == [2]


# Ensure that Custom Data Set classes can properly call non-overridden methods from their parent class
def test_base_class_expectation():
    df = CustomPandasDataset({
        "aaa": [1, 2, 3, 4, 5],
        "bbb": [10, 20, 30, 40, 50],
        "ccc": [9, 10, 11, 12, 13],
    })

    assert df.expect_column_values_to_be_between("aaa", min_value=1, max_value=5).success is True


def test_validate():

    with open("./tests/test_sets/titanic_expectations.json") as f:
        my_expectation_suite = expectationSuiteSchema.loads(f.read()).data

    my_df = ge.read_csv(
        "./tests/test_sets/Titanic.csv",
        expectation_suite=my_expectation_suite
    )
    my_df.set_default_expectation_argument("result_format", "COMPLETE")

    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        results = my_df.validate(catch_exceptions=False)

    with open('./tests/test_sets/titanic_expected_data_asset_validate_results.json') as f:
        expected_results = expectationSuiteValidationResultSchema.loads(f.read()).data

    del results.meta["great_expectations.__version__"]

    # order is not guaranteed (or important in this case) but sorting is possible in PY2
    if PY2:
        results.results = sorted(results.results)
        expected_results.results = sorted(expected_results.results)

    assert expected_results == results

    # Now, change the results and ensure they are no longer equal
    results.results[0] = ExpectationValidationResult()
    assert expected_results != results

    # Finally, confirm that only_return_failures works
    # and does not affect the "statistics" field.
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        validation_results = my_df.validate(only_return_failures=True)
        del validation_results.meta["great_expectations.__version__"]

    expected_results = ExpectationSuiteValidationResult(
        meta={
            "data_asset_name": "titanic",
            "expectation_suite_name": "default",
            "run_id": "19551105T000000.000000Z"
        },
        results=[ExpectationValidationResult(
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "PClass", "value_set": ["1st", "2nd", "3rd"]}
            ),
            success=False,
            exception_info={
                "exception_message": None,
                "exception_traceback": None,
                "raised_exception": False
            },
            result={
                "partial_unexpected_index_list": [456], "unexpected_count": 1, "unexpected_list": ["*"],
                "unexpected_percent": 0.07616146230007616, "element_count": 1313,
                "missing_percent": 0.0, "partial_unexpected_counts": [{"count": 1, "value": "*"}],
                "partial_unexpected_list": ["*"],
                "unexpected_percent_nonmissing": 0.07616146230007616, "missing_count": 0,
                "unexpected_index_list": [456]
            })
        ],
        success=expected_results.success,  # unaffected
        statistics=expected_results["statistics"]  # unaffected
    )
    assert expected_results == validation_results


def test_validate_catch_non_existent_expectation():
    df = ge.dataset.PandasDataset({
        "x": [1, 2, 3, 4, 5]
    })

    validation_config_non_existent_expectation = ExpectationSuite(
        data_asset_name="test",
        expectation_suite_name="default",
        meta={
            "great_expectations.__version__": ge.__version__
        },
        expectations=[ExpectationConfiguration(
            expectation_type="non_existent_expectation",
            kwargs={
                "column": "x"
            }
        )]
    )
    results = df.validate(expectation_suite=validation_config_non_existent_expectation)

    assert "object has no attribute 'non_existent_expectation'" in \
        results.results[0].exception_info['exception_message']


def test_validate_catch_invalid_parameter():
    df = ge.dataset.PandasDataset({
        "x": [1, 2, 3, 4, 5]
    })

    validation_config_invalid_parameter = ExpectationSuite(
        data_asset_name="test",
        expectation_suite_name="default",
        meta={
            "great_expectations.__version__": ge.__version__
        },
        expectations=[ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "x",
                "min_value": 6,
                "max_value": 5
            }
        )]
    )

    result = df.validate(expectation_suite=validation_config_invalid_parameter)
    assert "min_value cannot be greater than max_value" in result.results[0].exception_info['exception_message']


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
    expectation_results = [
        ExpectationValidationResult(success=False)
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(1, 0, 1, 0., False)
    assert expected == actual

    expectation_results = [
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=False)
    ]
    actual = _calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(3, 0, 3, 0., False)
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
        ExpectationValidationResult(success=True)
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


class TestIO(unittest.TestCase):

    def test_read_csv(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_csv(
            script_path+'/test_sets/Titanic.csv',
        )

    def test_read_json(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_json(
            script_path+'/test_sets/test_json_data_file.json',
        )

        df = ge.read_json(
            script_path+'/test_sets/nested_test_json_data_file.json',
            accessor_func=lambda x: x["data"]
        )

    def test_read_excel(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_excel(
            script_path+'/test_sets/Titanic_multi_sheet.xlsx',
        )
        assert df['Name'][0] == 'Allen, Miss Elisabeth Walton'
        assert isinstance(df, PandasDataset)

        # Note that pandas changed the parameter name from sheetname to sheet_name.
        # We will test with both options to ensure that the versions are correct.
        pandas_version = pd.__version__
        if re.match('0\.2[012]\.', pandas_version) is not None:
            dfs_dict = ge.read_excel(
                script_path+'/test_sets/Titanic_multi_sheet.xlsx',
                sheetname=None
            )
        else:
            dfs_dict = ge.read_excel(
                script_path+'/test_sets/Titanic_multi_sheet.xlsx',
                sheet_name=None
            )
        assert isinstance(dfs_dict, dict)
        assert list(dfs_dict.keys()) == ['Titanic_1', 'Titanic_2', 'Titanic_3']
        assert isinstance(dfs_dict['Titanic_1'], PandasDataset)
        assert dfs_dict['Titanic_1']['Name'][0] == 'Allen, Miss Elisabeth Walton'

    def test_read_table(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_table(
            script_path+'/test_sets/Titanic.csv',
            sep=','
        )
        assert df['Name'][0] == 'Allen, Miss Elisabeth Walton'
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
        pandas_version = re.match('0\.(.*)\..*', pd.__version__)
        if pandas_version is None:
            raise ValueError("Unrecognized pandas version!")
        else:
            pandas_version = int(pandas_version.group(1))
            if pandas_version < 21:
                return

        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_parquet(
            script_path+'/test_sets/Titanic.parquet'
        )
        assert df['Name'][1] == 'Allen, Miss Elisabeth Walton'
        assert isinstance(df, PandasDataset)


if __name__ == "__main__":
    unittest.main()
