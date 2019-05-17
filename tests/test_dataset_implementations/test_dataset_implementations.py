import json
import os

from ..test_utils import CONTEXTS, get_dataset, candidate_getter_is_on_temporary_notimplemented_list

import pytest


dir_path = os.path.dirname(os.path.realpath(__file__))
test_config_path = os.path.join(dir_path, 'test_config.json')
test_config = json.load(open(test_config_path))
data = test_config['data']
schemas = test_config.get('schemas', {})

@pytest.mark.parametrize('context', CONTEXTS)
@pytest.mark.parametrize('test', test_config['tests'])
def test_implementations(context, test):
    should_skip = (
        candidate_getter_is_on_temporary_notimplemented_list(context, test['func'])
        or
        context in test.get('supress_test_for', [])
    )
    if should_skip:
        pytest.xfail()
    dataset = get_dataset(context, data, schemas=schemas.get(context))
    func = getattr(dataset, test['func'])
    assert test['expected'] == func(**test['kwargs'])
