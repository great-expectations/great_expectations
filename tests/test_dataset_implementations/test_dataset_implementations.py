import json
import os

from ..test_utils import CONTEXTS, get_dataset, candidate_getter_is_on_temporary_notimplemented_list

import pytest


dir_path = os.path.dirname(os.path.realpath(__file__))
test_config_path = os.path.join(dir_path, 'test_dataset_implementations.json')
test_config = json.load(open(test_config_path))
test_datasets = test_config['test_datasets']

def generate_ids(test):
    return ':'.join([test['dataset'], test['func']])

@pytest.mark.parametrize('context', CONTEXTS)
@pytest.mark.parametrize('test', test_config['tests'], ids=[generate_ids(t) for t in test_config['tests']])
def test_implementations(context, test):
    should_skip = (
        candidate_getter_is_on_temporary_notimplemented_list(context, test['func'])
        or
        context in test.get('supress_test_for', [])
    )
    if should_skip:
        pytest.xfail()
    data = test_datasets[test['dataset']]['data']
    schema = test_datasets[test['dataset']]['schemas'].get(context)
    dataset = get_dataset(context, data, schemas=schema)
    func = getattr(dataset, test['func'])
    assert test['expected'] == func(**test.get('kwargs', {}))
