from __future__ import division

import os
import pytest

from tests.util import discover_json_test_configurations, evaluate_json_test

dataset_implementations = ['PandasDataSet', 'SqlAlchemyDataSet']
#test_config_dir = './tests/test_configurations/column_map_expectations'
test_config_dir = os.path.dirname(os.path.realpath(__file__)) + '/'
test_configurations, test_configuration_ids = discover_json_test_configurations(test_config_dir)

@pytest.fixture(scope="module",
                params=dataset_implementations)
def dataset_type(request):
    return request.param


@pytest.fixture(scope="module",
                params=test_configurations,
                ids=test_configuration_ids)
def test_configuration(request):
    return request.param


def test_column_map_expectations(test_configuration, dataset_type):
    evaluate_json_test(test_configuration, dataset_type)

