from __future__ import division

import pandas as pd
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
        yield SqlAlchemyDataSet(engine, 'test_data')
        engine.close()
    else:
        raise ValueError("Unknown dataset_type " + str(dataset_type))



@pytest.fixture(scope="module",
                params=dataset_implementations)
def test_expect_column_values_to_be_in_set_dataset(request):
    data_dict = {
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        }
    dataset = get_dataset(request.param, data_dict)
    yield dataset

def test_expect_column_values_to_be_in_set(test_expect_column_values_to_be_in_set_dataset):
    dataset = next(test_expect_column_values_to_be_in_set_dataset)
    dataset.set_default_expectation_argument("result_format", "COMPLETE")

    test_cases = [
        {
            'in': ['x', [1, 2, 4]],
            'out': {'success': True, 'unexpected_index_list': [], 'unexpected_list': []}},
        {
            'in': ['x', [4, 2]],
            'out': {'success': False, 'unexpected_index_list': [0], 'unexpected_list': [1]}},
        {
            'in': ['y', []],
            'out': {'success': False, 'unexpected_index_list': [0, 1, 2], 'unexpected_list': [1, 2, 5]}},
        {
            'in': ['z', ['hello', 'jello', 'mello']],
            'out': {'success': True, 'unexpected_index_list': [], 'unexpected_list': []}},
        {
            'in': ['z', ['hello']],
            'out': {'success': False, 'unexpected_index_list': [1, 2], 'unexpected_list': ['jello', 'mello']}}
    ]

    for test in test_cases:
        out = dataset.expect_column_values_to_be_in_set(*test['in'])
        assert test['out']['success'] == out['success']
        # assert test['out']['unexpected_index_list'] == out['result_obj']['unexpected_index_list']
        assert test['out']['unexpected_list'] == out['result_obj']['unexpected_list']