from __future__ import division

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

from great_expectations.dataset import PandasDataset, SqlAlchemyDataset

## Taken from the following stackoverflow: https://stackoverflow.com/questions/23549419/assert-that-two-dictionaries-are-almost-equal
def assertDeepAlmostEqual(test_case, expected, actual, *args, **kwargs):
    """
    Assert that two complex structures have almost equal contents.

    Compares lists, dicts and tuples recursively. Checks numeric values
    using test_case's :py:meth:`unittest.TestCase.assertAlmostEqual` and
    checks all other values with :py:meth:`unittest.TestCase.assertEqual`.
    Accepts additional positional and keyword arguments and pass those
    intact to assertAlmostEqual() (that's how you specify comparison
    precision).

    :param test_case: TestCase object on which we can call all of the basic
    'assert' methods.
    :type test_case: :py:class:`unittest.TestCase` object
    """
    is_root = not '__trace' in kwargs
    trace = kwargs.pop('__trace', 'ROOT')
    try:
       # if isinstance(expected, (int, float, long, complex)):
        if isinstance(expected, (int, float, complex)):
            test_case.assertAlmostEqual(expected, actual, *args, **kwargs)
        elif isinstance(expected, (list, tuple, np.ndarray)):
            test_case.assertEqual(len(expected), len(actual))
            for index in range(len(expected)):
                v1, v2 = expected[index], actual[index]
                assertDeepAlmostEqual(test_case, v1, v2,
                                      __trace=repr(index), *args, **kwargs)
        elif isinstance(expected, dict):
            test_case.assertEqual(set(expected), set(actual))
            for key in expected:
                assertDeepAlmostEqual(test_case, expected[key], actual[key],
                                      __trace=repr(key), *args, **kwargs)
        else:
            test_case.assertEqual(expected, actual)
    except AssertionError as exc:
        exc.__dict__.setdefault('traces', []).append(trace)
        if is_root:
            trace = ' -> '.join(reversed(exc.traces))
            exc = AssertionError("%s\nTRACE: %s" % (str(exc), trace))
        raise exc


def get_dataset(dataset_type, data):
    """For Pandas, data should be either a DataFrame or a dictionary that can be instantiated as a DataFrame
    For SQL, data should have the following shape:
        {
            'table':
                'table': SqlAlchemy Table object
                named_column: [list of values]
        }

    """
    if dataset_type == 'PandasDataset':
        return PandasDataset(data)
    elif dataset_type == 'SqlAlchemyDataset':
        # Create a new database

        engine = create_engine('sqlite://')

        # Add the data to the database as a new table
        df = pd.DataFrame(data)
        df.to_sql(name='test_data', con=engine, index=False)

        # Build a SqlAlchemyDataset using that database
        return SqlAlchemyDataset('test_data', engine=engine)
    else:
        raise ValueError("Unknown dataset_type " + str(dataset_type))


def candidate_test_is_on_temporary_notimplemented_list(context, expectation_type):
    if context == "SqlAlchemyDataset":
        return expectation_type in [
            #"expect_column_to_exist",
            #"expect_table_row_count_to_be_between",
            #"expect_table_row_count_to_equal",
            #"expect_table_columns_to_match_ordered_list",
            "expect_column_values_to_be_unique",
            # "expect_column_values_to_not_be_null",
            # "expect_column_values_to_be_null",
            "expect_column_values_to_be_of_type",
            "expect_column_values_to_be_in_type_list",
            # "expect_column_values_to_be_in_set",
            # "expect_column_values_to_not_be_in_set",
            # "expect_column_values_to_be_between",
            "expect_column_values_to_be_increasing",
            "expect_column_values_to_be_decreasing",
            # "expect_column_value_lengths_to_be_between",
            # "expect_column_value_lengths_to_equal",
            "expect_column_values_to_match_regex",
            "expect_column_values_to_not_match_regex",
            "expect_column_values_to_match_regex_list",
            "expect_column_values_to_not_match_regex_list",
            "expect_column_values_to_match_strftime_format",
            "expect_column_values_to_be_dateutil_parseable",
            "expect_column_values_to_be_json_parseable",
            "expect_column_values_to_match_json_schema",
            #"expect_column_mean_to_be_between",
            #"expect_column_median_to_be_between",
            "expect_column_stdev_to_be_between",
            #"expect_column_unique_value_count_to_be_between",
            #"expect_column_proportion_of_unique_values_to_be_between",
            "expect_column_most_common_value_to_be_in_set",
            #"expect_column_sum_to_be_between",
            #"expect_column_min_to_be_between",
            #"expect_column_max_to_be_between",
            "expect_column_chisquare_test_p_value_to_be_greater_than",
            "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
            "expect_column_kl_divergence_to_be_less_than",
            "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
            "expect_column_pair_values_to_be_equal",
            "expect_column_pair_values_A_to_be_greater_than_B",
            "expect_column_pair_values_to_be_in_set",
        ]
    return False


def evaluate_json_test(dataset, expectation_type, test):
    """
    This method will evaluate the result of a test build using the Great Expectations json test format.

    :param dataset: (Dataset) A great expectations Dataset
    :param expectation_type: (string) the name of the expectation to be run using the test input
    :param test: (dict) a dictionary containing information for the test to be run. The dictionary must include:
        - title: (string) the name of the test
        - exact_match_out: (boolean) If true, match the 'out' dictionary exactly against the result of the expectation
        - in: (dict or list) a dictionary of keyword arguments to use to evaluate the expectation or a list of positional arguments
        - out: (dict) the dictionary keys against which to make assertions. Unless exact_match_out is true, keys must\
            come from the following list:
              - success
              - observed_value
              - unexpected_index_list
              - unexpected_list
              - details
              - traceback_substring (if present, the string value will be expected as a substring of the exception_traceback)
    :return: None. asserts correctness of results.
    """

    dataset.set_default_expectation_argument('result_format', 'COMPLETE')

    if 'title' not in test:
        raise ValueError("Invalid test configuration detected: 'title' is required.")

    if 'exact_match_out' not in test:
        raise ValueError("Invalid test configuration detected: 'exact_match_out' is required.")

    if 'in' not in test:
        raise ValueError("Invalid test configuration detected: 'in' is required.")

    if 'out' not in test:
        raise ValueError("Invalid test configuration detected: 'out' is required.")

    # Pass the test if we are in a test condition that is a known exception

    # Known condition: SqlAlchemy does not support parse_strings_as_datetimes
    if 'parse_strings_as_datetimes' in test['in'] and isinstance(dataset, SqlAlchemyDataset):
        return

    # Known condition: SqlAlchemy does not support allow_cross_type_comparisons
    if 'allow_cross_type_comparisons' in test['in'] and isinstance(dataset, SqlAlchemyDataset):
        return

    try:
        # Support tests with positional arguments
        if isinstance(test['in'], list):
            result = getattr(dataset, expectation_type)(*test['in'])
        # As well as keyword arguments
        else:
            result = getattr(dataset, expectation_type)(**test['in'])

    except NotImplementedError:
        #Note: This method of checking does not look for false negatives: tests that are incorrectly on the notimplemented_list
        assert candidate_test_is_on_temporary_notimplemented_list(dataset.__class__.__name__, expectation_type), "Error: this test was supposed to return NotImplementedError"
        return

    # Check results
    if test['exact_match_out'] is True:
        assert test['out'] == result

    else:
        for key, value in test['out'].items():
            # Apply our great expectations-specific test logic

            if key == 'success':
                assert result['success'] == value

            elif key == 'observed_value':
                # assert np.allclose(result['result']['observed_value'], value)
                assert value == result['result']['observed_value']

            elif key == 'unexpected_index_list':
                if isinstance(dataset, SqlAlchemyDataset):
                    pass
                else:
                    assert result['result']['unexpected_index_list'] == value

            elif key == 'unexpected_list':
                assert result['result']['unexpected_list'] == value, "expected " + str(value) + " but got " + str(result['result']['unexpected_list'])

            elif key == 'details':
                assert result['result']['details'] == value

            elif key == 'traceback_substring':
                assert result['exception_info']['raised_exception']
                assert value in result['exception_info']['exception_traceback'], "expected to find " + value + " in " + result['exception_info']['exception_traceback']

            else:
                raise ValueError("Invalid test specification: unknown key " + key + " in 'out'")
