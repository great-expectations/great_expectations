from __future__ import division

import pytest

from .util import get_dataset, discover_json_test_configurations

dataset_implementations = ['PandasDataSet', 'SqlAlchemyDataSet']
test_config_dir = './tests/test_configurations/'

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


def test_runner(test_configuration, dataset_type):
    # Get the test configuration
    # with open(test_config_dir + test_configuration_file) as configuration_file:
    #     test_configuration = json.load(configuration_file)

    # Build the required dataset
    dataset = get_dataset(dataset_type, test_configuration['dataset'])
    dataset.set_default_expectation_argument('result_format', 'COMPLETE')

    # Get the expectation to run
    expectation_name = test_configuration['expectation']

    # Run the expectation and get results
    # for test in test_configuration['tests']:
    test = test_configuration['test']

    # Support tests with positional arguments
    if isinstance(test['in'], list):
        result = getattr(dataset, expectation_name)(*test['in'])
    # As well as keyword arguments
    else:
        result = getattr(dataset, expectation_name)(**test['in'])

    # Check results
    if 'out' in test:
        assert result['success'] == test['out']['success']

        # Handle dataset-specific implementation differences here
        if 'unexpected_index_list' in test['out']:
            if dataset_type == 'SqlAlchemyDataSet':
                pass
            else:
                assert result['result_obj']['unexpected_index_list'] == test['out']['unexpected_index_list']

        if 'unexpected_list' in test['out']:
            assert result['result_obj']['unexpected_list'] == test['out']['unexpected_list']

    if 'error' in test:
        assert result['exception_info']['raised_exception'] == True
        assert test['error']['traceback_substring'] in result['excpetion_info']['exception_traceback']