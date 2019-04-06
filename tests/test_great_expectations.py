import json
import os
import random
import unittest
import math

import pandas as pd
import re

import great_expectations as ge
from great_expectations.dataset.autoinspect import columns_exist
from great_expectations.dataset import PandasDataset, MetaPandasDataset
from great_expectations.data_asset.base import (
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


class TestCustomClass(unittest.TestCase):

    def test_custom_class(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        df = ge.read_csv(
            script_path+'/test_sets/Titanic.csv',
            dataset_class=CustomPandasDataset
        )
        df.set_default_expectation_argument("result_format", "COMPLETE")
        self.assertEqual(
            df.expect_column_values_to_be_prime(
                'Age')['result']['unexpected_list'],
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
        )

        primes = [3, 5, 7, 11, 13, 17, 23, 31]
        df["primes"] = df.Age.map(lambda x: random.choice(primes))
        self.assertEqual(
            df.expect_column_values_to_be_prime(
                "primes")['result']['unexpected_list'],
            []
        )

    def test_custom_expectation(self):
        df = CustomPandasDataset({'x': [1, 1, 1, 1, 2]})
        df.set_default_expectation_argument("result_format", "COMPLETE")

        out = df.expect_column_values_to_be_prime('x')
        t = {'out': {'unexpected_list': [1, 1, 1, 1], 'unexpected_index_list': [
            0, 1, 2, 3], 'success': False}}
        self.assertEqual(t['out']['success'], out['success'])
        if 'unexpected_index_list' in t['out']:
            self.assertEqual(t['out']['unexpected_index_list'],
                             out['result']['unexpected_index_list'])
        if 'unexpected_list' in t['out']:
            self.assertEqual(t['out']['unexpected_list'],
                             out['result']['unexpected_list'])

        out = df.expect_column_values_to_equal_1('x', mostly=.8)
        print(out)
        t = {'out': {'unexpected_list': [
            2], 'unexpected_index_list': [4], 'success': True}}
        self.assertEqual(t['out']['success'], out['success'])
        if 'unexpected_index_list' in t['out']:
            self.assertEqual(t['out']['unexpected_index_list'],
                             out['result']['unexpected_index_list'])
        if 'unexpected_list' in t['out']:
            self.assertEqual(t['out']['unexpected_list'],
                             out['result']['unexpected_list'])

   # Ensure that Custom Data Set classes can properly call non-overridden methods from their parent class
    def test_base_class_expectation(self):
        df = CustomPandasDataset({
            "aaa": [1, 2, 3, 4, 5],
            "bbb": [10, 20, 30, 40, 50],
            "ccc": [9, 10, 11, 12, 13],
        })

        self.assertEqual(
            df.expect_column_values_to_be_between(
                "aaa", min_value=1, max_value=5)['success'],
            True
        )


class TestValidation(unittest.TestCase):
    def test_validate(self):

        with open("./tests/test_sets/titanic_expectations.json") as f:
            my_expectations_config = json.load(f)

        my_df = ge.read_csv(
            "./tests/test_sets/Titanic.csv",
            expectations_config=my_expectations_config
        )
        my_df.set_default_expectation_argument("result_format", "COMPLETE")

        results = my_df.validate(catch_exceptions=False)
        # print json.dumps(results, indent=2)

        with open('./tests/test_sets/expected_results_20180303.json') as f:
            expected_results = json.load(f)
            # print json.dumps(expected_results, indent=2)

        self.maxDiff = None
        assertDeepAlmostEqual(
            results,
            expected_results
        )

        # Now, change the results and ensure they are no longer equal
        results[0] = {}
        self.assertNotEqual(results,
                            expected_results
                            )

        # Finally, confirm that only_return_failures works
        # and does not affect the "statistics" field.
        validation_results = my_df.validate(only_return_failures=True)
        # print json.dumps(validation_results)
        assertDeepAlmostEqual(
            validation_results,
            {"results": [
                {"expectation_config": {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "PClass", "value_set": ["1st", "2nd", "3rd"], "result_format": "COMPLETE"}
                },
                    "success": False,
                    "exception_info": {"exception_message": None,
                                       "exception_traceback": None,
                                       "raised_exception": False},
                    "result": {"partial_unexpected_index_list": [456], "unexpected_count": 1, "unexpected_list": ["*"],
                               "unexpected_percent": 0.0007616146230007616, "element_count": 1313,
                               "missing_percent": 0.0, "partial_unexpected_counts": [{"count": 1, "value": "*"}],
                               "partial_unexpected_list": ["*"],
                               "unexpected_percent_nonmissing": 0.0007616146230007616, "missing_count": 0,
                               "unexpected_index_list": [456]}}
            ],
                "success": expected_results["success"],  # unaffected
                "statistics": expected_results["statistics"],  # unaffected
            }
        )

    def test_validate_catch_non_existent_expectation(self):
        df = ge.dataset.PandasDataset({
            "x": [1, 2, 3, 4, 5]
        })

        validation_config_non_existent_expectation = {
            "dataset_name": None,
            "meta": {
                "great_expectations.__version__": ge.__version__
            },
            "expectations": [{
                "expectation_type": "non_existent_expectation",
                "kwargs": {
                    "column": "x"
                }
            }]
        }
        results = df.validate(
            expectations_config=validation_config_non_existent_expectation)['results']

        self.assertIn(
            "object has no attribute 'non_existent_expectation'",
            results[0]['exception_info']['exception_message']
        )

    def test_validate_catch_invalid_parameter(self):
        df = ge.dataset.PandasDataset({
            "x": [1, 2, 3, 4, 5]
        })

        validation_config_invalid_parameter = {
            "dataset_name": None,
            "meta": {
                "great_expectations.__version__": ge.__version__
            },
            "expectations": [{
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "x",
                    "min_value": 6,
                    "max_value": 5
                }
            }]
        }

        results = df.validate(expectations_config=validation_config_invalid_parameter)[
            'results']
        print(results[0]['exception_info'])
        self.assertIn(
            "min_value cannot be greater than max_value",
            results[0]['exception_info']['exception_message']
        )

    def test_top_level_validate(self):
        my_df = pd.DataFrame({
            "x": [1, 2, 3, 4, 5]
        })
        validation_result = ge.validate(my_df, {
            "dataset_name": None,
            "meta": {
                "great_expectations.__version__": ge.__version__
            },
            "expectations": [{
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": "x"
                }
            }, {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "x",
                    "min_value": 3,
                    "max_value": 5
                }
            }]
        })
        self.assertEqual(
            validation_result,
            {
                "results": [
                    {
                        "expectation_config": {
                            "kwargs": {
                                "column": "x"
                            },
                            "expectation_type": "expect_column_to_exist",
                        },
                        "exception_info": {"exception_message": None,
                                           "exception_traceback": None,
                                           "raised_exception": False},
                        "success": True
                    },
                    {
                        "expectation_config": {
                            "expectation_type": "expect_column_values_to_be_between",
                            "kwargs": {
                                "column": "x",
                                "max_value": 5,
                                "min_value": 3
                            }
                        },
                        "exception_info": {"exception_message": None,
                                           "exception_traceback": None,
                                           "raised_exception": False},
                        "success": False,
                        "result": {'element_count': 5,
                                   'missing_count': 0,
                                   'missing_percent': 0.0,
                                   "unexpected_percent": 0.4,
                                   "partial_unexpected_list": [
                                       1,
                                       2
                                   ],
                                   "unexpected_percent_nonmissing": 0.4,
                                   "unexpected_count": 2
                                   }
                    }
                ],
                "success": False,
                "statistics": {
                    "evaluated_expectations": 2,
                    "successful_expectations": 1,
                    "unsuccessful_expectations": 1,
                    "success_percent": 50,
                }
            }
        )


class TestValidationStatisticsCalculation(unittest.TestCase):
    def test_no_expectations(self):
        expectation_results = []
        actual = _calc_validation_statistics(expectation_results)

        # pay attention to these two
        self.assertTrue(math.isnan(actual.success_percent))
        self.assertEqual(actual.success, True)
        # the rest is boring
        self.assertEqual(actual.successful_expectations, 0)
        self.assertEqual(actual.evaluated_expectations, 0)
        self.assertEqual(actual.unsuccessful_expectations, 0)

    def test_no_succesful_expectations(self):
        expectation_results = [
            {"success": False},
        ]
        actual = _calc_validation_statistics(expectation_results)
        expected = ValidationStatistics(1, 0, 1, 0., False)
        assertDeepAlmostEqual(actual, expected)

        expectation_results = [
            {"success": False},
            {"success": False},
            {"success": False},
        ]
        actual = _calc_validation_statistics(expectation_results)
        expected = ValidationStatistics(3, 0, 3, 0., False)
        assertDeepAlmostEqual(actual, expected)

    def test_all_succesful_expectations(self):
        expectation_results = [
            {"success": True},
        ]
        actual = _calc_validation_statistics(expectation_results)
        expected = ValidationStatistics(1, 1, 0, 100.0, True)
        assertDeepAlmostEqual(actual, expected)

        expectation_results = [
            {"success": True},
            {"success": True},
            {"success": True},
        ]
        actual = _calc_validation_statistics(expectation_results)
        expected = ValidationStatistics(3, 3, 0, 100.0, True)
        assertDeepAlmostEqual(actual, expected)

    def test_mixed_expectations(self):
        expectation_results = [
            {"success": False},
            {"success": True},
        ]
        actual = _calc_validation_statistics(expectation_results)
        expected = ValidationStatistics(2, 1, 1, 50.0, False)
        assertDeepAlmostEqual(actual, expected)


class TestRepeatedAppendExpectation(unittest.TestCase):
    def test_validate(self):

        with open("./tests/test_sets/titanic_expectations.json") as f:
            my_expectations_config = json.load(f)

        my_df = ge.read_csv("./tests/test_sets/Titanic.csv",
                            autoinspect_func=columns_exist)

        self.assertEqual(
            len(my_df.get_expectations_config()['expectations']),
            7
        )

        # For column_expectations, _append_expectation should only replace expectations where the expetation_type AND the column match
        my_df.expect_column_to_exist("PClass")
        self.assertEqual(
            len(my_df.get_expectations_config()['expectations']),
            7
        )


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
