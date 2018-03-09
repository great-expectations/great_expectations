from __future__ import division
import pytest

from .util import get_dataset

dataset_implementations = ['PandasDataSet', 'SqlAlchemyDataSet']
test_configuration_files = ['expect_column_values_to_be_between_test_set.json']

@pytest.fixture(scope="module",
                params=dataset_implementations)
def dataset_builder(request):
    data_dict = {
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        }
    dataset = get_dataset(request.param, data_dict)
    yield dataset

@pytest.fixture(scope="module", params=test_configuration_files)
def test_runner(request, dataset_builder):
    with open("./tests/test_sets/" + request.param + ".json") as f:

    with open(request.param) as test_configuration:
        dataset = dataset_builder


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