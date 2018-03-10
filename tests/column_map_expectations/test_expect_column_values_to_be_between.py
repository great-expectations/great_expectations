###
###
#
# This file should not be modified. To adjust test cases, edit the related json file.
#
###
###


import pytest

import os
import json

from tests.util import get_dataset, evaluate_json_test

file = open(os.path.basename(__file__)[5:-3] + '.json')
test_configurations = json.load(file)

dataset = test_configurations['dataset']
expectation_name = test_configurations['expectation_type']
test_cases = test_configurations['tests']
test_case_ids = [test['title'] for test in test_cases]


@pytest.fixture(scope="module")
def test_data(dataset_type):
    return get_dataset(dataset_type, dataset)


@pytest.fixture(scope="module",
                params=test_cases,
                ids=test_case_ids)
def test_case(request):
    return request.param


def test_case_runner(test_data, test_case):
    evaluate_json_test(test_data, expectation_name, test_case)