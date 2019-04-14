from __future__ import division

import random
import string
import warnings

import pandas as pd
import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy.dialects.sqlite as sqlitetypes
import sqlalchemy.dialects.postgresql as postgresqltypes

from great_expectations.dataset import PandasDataset, SqlAlchemyDataset
import great_expectations.dataset.autoinspect as autoinspect

SQLITE_TYPES = {
        "varchar": sqlitetypes.VARCHAR,
        "char": sqlitetypes.CHAR,
        "int": sqlitetypes.INTEGER,
        "smallint": sqlitetypes.SMALLINT,
        "datetime": sqlitetypes.DATETIME(truncate_microseconds=True),
        "date": sqlitetypes.DATE,
        "float": sqlitetypes.FLOAT,
        "bool": sqlitetypes.BOOLEAN
}

POSTGRESQL_TYPES = {
        "text": postgresqltypes.TEXT,
        "char": postgresqltypes.CHAR,
        "int": postgresqltypes.INTEGER,
        "smallint": postgresqltypes.SMALLINT,
        "timestamp": postgresqltypes.TIMESTAMP,
        "date": postgresqltypes.DATE,
        "float": postgresqltypes.FLOAT,
        "bool": postgresqltypes.BOOLEAN
}

# Taken from the following stackoverflow:
# https://stackoverflow.com/questions/23549419/assert-that-two-dictionaries-are-almost-equal
def assertDeepAlmostEqual(expected, actual, *args, **kwargs):
    """
    Assert that two complex structures have almost equal contents.

    Compares lists, dicts and tuples recursively. Checks numeric values
    using pyteset.approx and checks all other values with an assertion equality statement
    Accepts additional positional and keyword arguments and pass those
    intact to pytest.approx() (that's how you specify comparison
    precision).

    """
    is_root = '__trace' not in kwargs
    trace = kwargs.pop('__trace', 'ROOT')
    try:
        # if isinstance(expected, (int, float, long, complex)):
        if isinstance(expected, (int, float, complex)):
            assert expected == pytest.approx(actual, *args, **kwargs)
        elif isinstance(expected, (list, tuple, np.ndarray)):
            assert len(expected) == len(actual)
            for index in range(len(expected)):
                v1, v2 = expected[index], actual[index]
                assertDeepAlmostEqual(v1, v2,
                                      __trace=repr(index), *args, **kwargs)
        elif isinstance(expected, dict):
            assert set(expected) == set(actual)
            for key in expected:
                assertDeepAlmostEqual(expected[key], actual[key],
                                      __trace=repr(key), *args, **kwargs)
        else:
            assert expected == actual
    except AssertionError as exc:
        exc.__dict__.setdefault('traces', []).append(trace)
        if is_root:
            trace = ' -> '.join(reversed(exc.traces))
            exc = AssertionError("%s\nTRACE: %s" % (str(exc), trace))
        raise exc


def get_dataset(dataset_type, data, schemas=None, autoinspect_func=autoinspect.columns_exist):
    """For Pandas, data should be either a DataFrame or a dictionary that can
    be instantiated as a DataFrame.
    For SQL, data should have the following shape:
        {
            'table':
                'table': SqlAlchemy Table object
                named_column: [list of values]
        }

    """
    if dataset_type == 'PandasDataset':
        df = pd.DataFrame(data)
        if schemas and "pandas" in schemas:
            pandas_schema = {key:np.dtype(value) for (key, value) in schemas["pandas"].items()}
            df = df.astype(pandas_schema)
        return PandasDataset(df, autoinspect_func=autoinspect_func)
    elif dataset_type == 'SqlAlchemyDataset':
        # Create a new database

        # Try to use a local postgres instance (e.g. on Travis); this will allow more testing than sqlite
        try:
            engine = create_engine('postgresql://test:test@localhost/test_ci')
            conn = engine.connect()
        except SQLAlchemyError:
            warnings.warn("Falling back to sqlite database.")
            engine = create_engine('sqlite://')
            conn = engine.connect()

        # Add the data to the database as a new table
        df = pd.DataFrame(data)

        sql_dtypes = {}
        if schemas and "sqlite" in schemas and isinstance(engine.dialect, sqlitetypes.dialect):
            schema = schemas["sqlite"]
            sql_dtypes = {col : SQLITE_TYPES[dtype] for (col,dtype) in schema.items()}
            for col in schema:
                type = schema[col]
                if type == "int":
                    df[col] = pd.to_numeric(df[col],downcast='signed')
                elif type == "float":
                    df[col] = pd.to_numeric(df[col],downcast='float')
                elif type == "datetime":
                    df[col] = pd.to_datetime(df[col])
        elif schemas and "postgresql" in schemas and isinstance(engine.dialect, postgresqltypes.dialect):
            schema = schemas["postgresql"]
            sql_dtypes = {col : POSTGRESQL_TYPES[dtype] for (col, dtype) in schema.items()}
            for col in schema:
                type = schema[col]
                if type == "int":
                    df[col] = pd.to_numeric(df[col],downcast='signed')
                elif type == "float":
                    df[col] = pd.to_numeric(df[col],downcast='float')
                elif type == "timestamp":
                    df[col] = pd.to_datetime(df[col])

        tablename = "test_data_" + ''.join([random.choice(string.ascii_letters + string.digits) for n in range(8)])
        df.to_sql(name=tablename, con=conn, index=False, dtype=sql_dtypes)

        # Build a SqlAlchemyDataset using that database
        return SqlAlchemyDataset(tablename, engine=conn, autoinspect_func=autoinspect_func)
    else:
        raise ValueError("Unknown dataset_type " + str(dataset_type))


def candidate_test_is_on_temporary_notimplemented_list(context, expectation_type):
    if context == "SqlAlchemyDataset":
        return expectation_type in [
            #"expect_column_to_exist",
            #"expect_table_row_count_to_be_between",
            #"expect_table_row_count_to_equal",
            #"expect_table_columns_to_match_ordered_list",
            #"expect_column_values_to_be_unique",
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
            "expect_multicolumn_values_to_be_unique"
        ]
    return False


def evaluate_json_test(data_asset, expectation_type, test):
    """
    This method will evaluate the result of a test build using the Great Expectations json test format.

    NOTE: Tests can be suppressed for certain data types if the test contains the Key 'suppress_test_for' with a list
        of DataAsset types to suppress, such as ['SQLAlchemy', 'Pandas'].

    :param data_asset: (DataAsset) A great expectations DataAsset
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

    data_asset.set_default_expectation_argument('result_format', 'COMPLETE')

    if 'title' not in test:
        raise ValueError(
            "Invalid test configuration detected: 'title' is required.")

    if 'exact_match_out' not in test:
        raise ValueError(
            "Invalid test configuration detected: 'exact_match_out' is required.")

    if 'in' not in test:
        raise ValueError(
            "Invalid test configuration detected: 'in' is required.")

    if 'out' not in test:
        raise ValueError(
            "Invalid test configuration detected: 'out' is required.")

    # Support tests with positional arguments
    if isinstance(test['in'], list):
        result = getattr(data_asset, expectation_type)(*test['in'])
    # As well as keyword arguments
    else:
        result = getattr(data_asset, expectation_type)(**test['in'])

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
                if isinstance(data_asset, SqlAlchemyDataset):
                    pass
                else:
                    assert result['result']['unexpected_index_list'] == value

            elif key == 'unexpected_list':
                assert result['result']['unexpected_list'] == value, "expected " + \
                    str(value) + " but got " + \
                    str(result['result']['unexpected_list'])

            elif key == 'details':
                assert result['result']['details'] == value

            elif key == 'traceback_substring':
                assert result['exception_info']['raised_exception']
                assert value in result['exception_info']['exception_traceback'], "expected to find " + \
                    value + " in " + \
                    result['exception_info']['exception_traceback']

            else:
                raise ValueError(
                    "Invalid test specification: unknown key " + key + " in 'out'")
