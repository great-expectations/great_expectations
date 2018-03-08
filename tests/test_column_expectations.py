from __future__ import division

import pandas as pd
import numpy as np
from sqlalchemy import create_engine


from great_expectations.dataset import PandasDataSet, SqlAlchemyDataSet
from .util import parse_expectation_result


import pytest

dataset_implementations = ['PandasDataSet', 'SqlAlchemyDataSet']


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
        yield PandasDataSet(data)
    elif dataset_type == 'SqlAlchemyDataSet':
        # Create a new database

        engine = create_engine('sqlite://')

        # Add the data to the database as a new table
        df = pd.DataFrame(data)
        df.to_sql(name='test_data', con=engine)

        # Build a SqlAlchemyDataSet using that database
        yield SqlAlchemyDataSet('test_data', engine)
        engine.close()
    else:
        raise ValueError("Unknown dataset_type " + str(dataset_type))



@pytest.fixture(scope="module",
                params=dataset_implementations)
def test_expect_column_values_to_be_in_set_dataset(request):
    data_dict = {
        'a': ['2', '2'],
        'b': [1, '2'],
        'c': [1, 1],
        'd': [1, '1'],
        'n': [None, np.nan]
    }
    dataset = get_dataset(request.param, data_dict)
    yield dataset

def test_expect_column_values_to_be_unique(test_expect_column_values_to_be_unique_dataset):
    dataset = next(test_expect_column_values_to_be_unique_dataset)
    dataset.set_default_expectation_argument("result_format", "COMPLETE")

    # Tests for D
    T = [
        {
            'in': {'column': 'a'},
            'out': {'success': False, 'unexpected_index_list': [0, 1], 'unexpected_list': ['2', '2']}},
        {
            'in': {'column': 'b'},
            'out': {'success': True, 'unexpected_index_list': [], 'unexpected_list': []}},
        {
            'in': {'column': 'c'},
            'out': {'success': False, 'unexpected_index_list': [0, 1], 'unexpected_list': [1, 1]}},
        {
            'in': {'column': 'd'},
            'out': {'success': True, 'unexpected_index_list': [], 'unexpected_list': []}},
        {
            'in': {'column': 'n'},
            'out': {'success': True, 'unexpected_index_list': [], 'unexpected_list': []}}
    ]

    for t in T:
        out = dataset.expect_column_values_to_be_unique(**t['in'])
        assert t['out']['success'] == out['success']
        assert t['out']['unexpected_index_list'] == out['result_obj']['unexpected_index_list']
        assert t['out']['unexpected_list'] == out['result_obj']['unexpected_list']