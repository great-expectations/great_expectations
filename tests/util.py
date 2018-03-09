from __future__ import division

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
    dataset.set_default_expectation_argument('result_format', 'COMPLETE')

    if 'exception' in test:
        with pytest.raises(Exception) as exception_info:
            if isinstance(test['in'], list):
                getattr(dataset, expectation_type)(*test['in'])
            else:
                result = getattr(dataset, expectation_type)(**test['in'])
        assert test['exception'] in str(exception_info)

    elif 'error' in test:
        # Support tests with positional arguments
        if isinstance(test['in'], list):
            result = getattr(dataset, expectation_type)(*test['in'])
        # As well as keyword arguments
        else:
            result = getattr(dataset, expectation_type)(**test['in'])

        assert result['exception_info']['raised_exception'] == True
        assert test['error']['traceback_substring'] in result['exception_info']['exception_traceback']

    elif 'out' in test:
        # Support tests with positional arguments
        if isinstance(test['in'], list):
            result = getattr(dataset, expectation_type)(*test['in'])
        # As well as keyword arguments
        else:
            result = getattr(dataset, expectation_type)(**test['in'])

        # Check results
        if 'exact_compare_out' in test and (test['exact_compare_out'] == True):
            assert test['out'] == result

        else:
            assert result['success'] == test['out']['success']

            if 'result_obj' in test['out']:
                if 'observed_value' in test['out']['result_obj']:
                    assert result['result_obj']['observed_value'] == test['out']['result_obj']

                if 'details' in test['out']['result_obj']:
                    assert test['out']['result_obj']['details'] == result['result_obj']['details']

            # Handle dataset-specific implementation differences here
            if 'unexpected_index_list' in test['out']:
                if isinstance(dataset, SqlAlchemyDataSet):
                    pass
                else:
                    assert result['result_obj']['unexpected_index_list'] == test['out']['unexpected_index_list']

            if 'unexpected_list' in test['out']:
                assert result['result_obj']['unexpected_list'] == test['out']['unexpected_list']

    else:
        raise Exception("Malformed test: no 'out' or 'exception' section found.")