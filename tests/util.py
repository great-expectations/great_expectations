from __future__ import division

from six import string_types

import pandas as pd
import numpy as np
import os
import json
import pytest

import sqlalchemy.types
from sqlalchemy import create_engine

from great_expectations.dataset import PandasDataSet, SqlAlchemyDataSet

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
    if dataset_type == 'PandasDataSet':
        return PandasDataSet(data)
    elif dataset_type == 'SqlAlchemyDataSet':
        # Create a new database

        engine = create_engine('sqlite://')

        # Add the data to the database as a new table
        df = pd.DataFrame(data)
        df.to_sql(name='test_data', con=engine)

        # Build a SqlAlchemyDataSet using that database
        return SqlAlchemyDataSet(engine, 'test_data')
    else:
        raise ValueError("Unknown dataset_type " + str(dataset_type))


def evaluate_json_test(dataset, expectation_type, test):
    """
    This method will evaluate the result of a test build using the Great Expectations json test format.

    :param dataset: (DataSet) A great expectations DataSet
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
    :return:
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

    # Support tests with positional arguments
    if isinstance(test['in'], list):
        result = getattr(dataset, expectation_type)(*test['in'])
    # As well as keyword arguments
    else:
        result = getattr(dataset, expectation_type)(**test['in'])

    # Check results
    if test['exact_match_out'] is True:
        assert test['out'] == result

    else:
        for key, value in test['out'].items():
            # Apply our great expectations-specific test logic

            if key == 'success':
                assert result['success'] == value

            elif key == 'observed_value':
                assert np.all(result['result_obj']['observed_value'], value)

            elif key == 'unexpected_index_list':
                if isinstance(dataset, SqlAlchemyDataSet):
                    pass
                else:
                    assert result['result_obj']['unexpected_index_list'] == value

            elif key == 'unexpected_list':
                assert result['result_obj']['unexpected_list'] == value

            elif key == 'details':
                assert result['result_obj']['details'] == value

            elif key == 'traceback_substring':
                assert result['exception_info']['raised_exception']
                assert value in result['exception_info']

            else:
                raise ValueError("Invalid test specification: unknown key " + key + " in 'out'")